//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

use std::any::Any;
use std::mem::size_of;
use std::sync::Arc;

use chrono::NaiveDateTime;
use common_datablocks::DataBlock;
use common_datavalues::chrono::TimeZone;
use common_datavalues::chrono::Utc;
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_types::TableIdent;
use common_meta_types::TableInfo;
use common_meta_types::TableMeta;
use common_planners::Expression;
use common_planners::Extras;
use common_planners::PartInfoPtr;
use common_planners::Partitions;
use common_planners::ReadDataSourcePlan;
use common_planners::Statistics;
use common_streams::SendableDataBlockStream;

use super::numbers_stream::NumbersStream;
use crate::pipelines::new::processors::port::OutputPort;
use crate::pipelines::new::processors::processor::ProcessorPtr;
use crate::pipelines::new::processors::EmptySource;
use crate::pipelines::new::processors::SyncSource;
use crate::pipelines::new::processors::SyncSourcer;
use crate::pipelines::new::NewPipe;
use crate::pipelines::new::NewPipeline;
use crate::pipelines::new::SourcePipeBuilder;
use crate::pipelines::transforms::get_sort_descriptions;
use crate::sessions::QueryContext;
use crate::storages::Table;
use crate::table_functions::generate_numbers_parts;
use crate::table_functions::numbers_part::NumbersPartInfo;
use crate::table_functions::table_function_factory::TableArgs;
use crate::table_functions::TableFunction;

pub struct NumbersTable {
    table_info: TableInfo,
    total: u64,
}

impl NumbersTable {
    pub fn create(
        database_name: &str,
        table_func_name: &str,
        table_id: u64,
        table_args: TableArgs,
    ) -> Result<Arc<dyn TableFunction>> {
        let mut total = None;
        if let Some(args) = &table_args {
            if args.len() == 1 {
                let arg = &args[0];
                if let Expression::Literal { value, .. } = arg {
                    total = Some(value.as_u64()?);
                }
            }
        }

        let total = total.ok_or_else(|| {
            ErrorCode::BadArguments(format!(
                "Must have exactly one number argument for table function.{}",
                &table_func_name
            ))
        })?;

        let engine = match table_func_name {
            "numbers" => "SystemNumbers",
            "numbers_mt" => "SystemNumbersMt",
            "numbers_local" => "SystemNumbersLocal",
            _ => unreachable!(),
        };

        let table_info = TableInfo {
            ident: TableIdent::new(table_id, 0),
            desc: format!("'{}'.'{}'", database_name, table_func_name),
            name: table_func_name.to_string(),
            meta: TableMeta {
                schema: DataSchemaRefExt::create(vec![DataField::new(
                    "number",
                    u64::to_data_type(),
                )]),
                engine: engine.to_string(),
                // Assuming that created_on is unnecessary for function table,
                // we could make created_on fixed to pass test_shuffle_action_try_into.
                created_on: Utc.from_utc_datetime(&NaiveDateTime::from_timestamp(0, 0)),
                ..Default::default()
            },
        };

        Ok(Arc::new(NumbersTable { table_info, total }))
    }
}

#[async_trait::async_trait]
impl Table for NumbersTable {
    fn is_local(&self) -> bool {
        self.name() == "numbers_local"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    async fn read_partitions(
        &self,
        ctx: Arc<QueryContext>,
        push_downs: Option<Extras>,
    ) -> Result<(Statistics, Partitions)> {
        let max_block_size = ctx.get_settings().get_max_block_size()?;
        let mut limit = None;

        if let Some(extras) = &push_downs {
            if extras.limit.is_some() && extras.filters.is_empty() {
                let sort_descriptions_result =
                    get_sort_descriptions(&self.table_info.schema(), &extras.order_by);

                // It is allowed to have an error when we can't get sort columns from the expression. For
                // example 'select number from numbers(10) order by number+4 limit 10', the column 'number+4'
                // doesn't exist in the numbers table.
                // For case like that, we ignore the error and don't apply any optimization.

                // No order by case
                match sort_descriptions_result {
                    Ok(v) if v.is_empty() => {
                        limit = extras.limit;
                    }
                    _ => {}
                }
            }
        }
        let total = match limit {
            Some(limit) => std::cmp::min(self.total, limit as u64),
            None => self.total,
        };

        let fake_partitions = (total / max_block_size) + 1;
        let statistics = Statistics::new_exact(
            total as usize,
            ((total) * size_of::<u64>() as u64) as usize,
            fake_partitions as usize,
            fake_partitions as usize,
        );

        let mut worker_num = ctx.get_settings().get_max_threads()?;
        if worker_num > fake_partitions {
            worker_num = fake_partitions;
        }

        let parts = generate_numbers_parts(0, worker_num, total);
        Ok((statistics, parts))
    }

    fn table_args(&self) -> Option<Vec<Expression>> {
        Some(vec![Expression::create_literal(DataValue::UInt64(
            self.total,
        ))])
    }

    async fn read(
        &self,
        ctx: Arc<QueryContext>,
        _plan: &ReadDataSourcePlan,
    ) -> Result<SendableDataBlockStream> {
        Ok(Box::pin(NumbersStream::try_create(
            ctx,
            self.schema(),
            vec![],
            None,
        )?))
    }

    fn read2(
        &self,
        ctx: Arc<QueryContext>,
        plan: &ReadDataSourcePlan,
        pipeline: &mut NewPipeline,
    ) -> Result<()> {
        if plan.parts.is_empty() {
            let schema = plan.schema();
            let output = OutputPort::create();
            pipeline.add_pipe(NewPipe::SimplePipe {
                inputs_port: vec![],
                outputs_port: vec![output.clone()],
                processors: vec![EmptySource::create(ctx, output, schema)?],
            });

            return Ok(());
        }

        let mut source_builder = SourcePipeBuilder::create();

        for part_index in 0..plan.parts.len() {
            let source_ctx = ctx.clone();
            let source_output_port = OutputPort::create();

            source_builder.add_source(
                source_output_port.clone(),
                NumbersSource::create(
                    source_output_port,
                    source_ctx,
                    &plan.parts[part_index],
                    self.schema(),
                )?,
            );
        }

        pipeline.add_pipe(source_builder.finalize());
        Ok(())
    }
}

struct NumbersSource {
    begin: u64,
    end: u64,
    step: u64,
    schema: DataSchemaRef,
}

impl NumbersSource {
    pub fn create(
        output: Arc<OutputPort>,
        ctx: Arc<QueryContext>,
        numbers_part: &PartInfoPtr,
        schema: DataSchemaRef,
    ) -> Result<ProcessorPtr> {
        let settings = ctx.get_settings();
        let numbers_part = NumbersPartInfo::from_part(numbers_part)?;

        SyncSourcer::create(ctx, output, NumbersSource {
            schema,
            begin: numbers_part.part_start,
            end: numbers_part.part_end,
            step: settings.get_max_block_size()?,
        })
    }
}

impl SyncSource for NumbersSource {
    const NAME: &'static str = "numbers";

    fn generate(&mut self) -> Result<Option<DataBlock>> {
        let source_remain_size = self.end - self.begin;

        match source_remain_size {
            0 => Ok(None),
            remain_size => {
                let step = std::cmp::min(remain_size, self.step);
                let column_data = (self.begin..self.begin + step).collect();

                self.begin += step;
                let column = UInt64Column::new_from_vec(column_data);
                Ok(Some(DataBlock::create(self.schema.clone(), vec![
                    Arc::new(column),
                ])))
            }
        }
    }
}

impl TableFunction for NumbersTable {
    fn function_name(&self) -> &str {
        self.name()
    }

    fn as_table<'a>(self: Arc<Self>) -> Arc<dyn Table + 'a>
    where Self: 'a {
        self
    }
}
