// Copyright 2020 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::PlanNode;
use common_planners::TruncateTablePlan;
use sqlparser::ast::ObjectName;

use crate::sessions::DatabendQueryContextRef;
use crate::sql::statements::AnalyzableStatement;
use crate::sql::statements::AnalyzedResult;

#[derive(Debug, Clone, PartialEq)]
pub struct DfTruncateTable {
    pub name: ObjectName,
}

#[async_trait::async_trait]
impl AnalyzableStatement for DfTruncateTable {
    async fn analyze(&self, ctx: DatabendQueryContextRef) -> Result<AnalyzedResult> {
        let (db, table) = self.resolve_table(ctx)?;
        Ok(AnalyzedResult::SimpleQuery(PlanNode::TruncateTable(
            TruncateTablePlan { db, table },
        )))
    }
}

impl DfTruncateTable {
    fn resolve_table(&self, ctx: DatabendQueryContextRef) -> Result<(String, String)> {
        let DfTruncateTable {
            name: ObjectName(idents),
            ..
        } = self;
        match idents.len() {
            0 => Err(ErrorCode::SyntaxException("Truncate table name is empty")),
            1 => Ok((ctx.get_current_database(), idents[0].value.clone())),
            2 => Ok((idents[0].value.clone(), idents[1].value.clone())),
            _ => Err(ErrorCode::SyntaxException(
                "Truncate table name must be [`db`].`table`",
            )),
        }
    }
}
