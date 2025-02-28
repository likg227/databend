// Copyright 2022 Datafuse Labs.
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

use std::collections::hash_map::Entry;
use std::collections::HashMap;

use common_ast::ast::Expr;
use common_ast::ast::Literal;
use common_ast::ast::SelectTarget;
use common_ast::parser::error::DisplayError;
use common_datavalues::DataTypeImpl;
use common_exception::ErrorCode;
use common_exception::Result;

use super::scalar_common::split_conjunctions;
use crate::sql::binder::scalar::ScalarBinder;
use crate::sql::binder::select::SelectList;
use crate::sql::binder::Binder;
use crate::sql::binder::ColumnBinding;
use crate::sql::optimizer::SExpr;
use crate::sql::planner::metadata::MetadataRef;
use crate::sql::planner::semantic::GroupingChecker;
use crate::sql::plans::AggregateFunction;
use crate::sql::plans::AggregatePlan;
use crate::sql::plans::AndExpr;
use crate::sql::plans::BoundColumnRef;
use crate::sql::plans::CastExpr;
use crate::sql::plans::ComparisonExpr;
use crate::sql::plans::EvalScalar;
use crate::sql::plans::FilterPlan;
use crate::sql::plans::FunctionCall;
use crate::sql::plans::OrExpr;
use crate::sql::plans::Scalar;
use crate::sql::plans::ScalarExpr;
use crate::sql::plans::ScalarItem;
use crate::sql::BindContext;

#[derive(Default, Clone, PartialEq, Debug)]
pub struct AggregateInfo {
    /// Aggregation functions
    pub aggregate_functions: Vec<ScalarItem>,

    /// Arguments of aggregation functions
    pub aggregate_arguments: Vec<ScalarItem>,

    /// Group items of aggregation
    pub group_items: Vec<ScalarItem>,

    /// Output columns of aggregation, including group items and aggregate functions.
    pub output_columns: Vec<ColumnBinding>,

    /// Mapping: (aggregate function display name) -> (index of agg func in `aggregate_functions`)
    /// This is used to find a aggregate function in current context.
    pub aggregate_functions_map: HashMap<String, usize>,

    /// Mapping: (group item display name) -> (index of group item in `group_items`)
    /// This is used to check if a scalar expression is a group item.
    /// For example, `SELECT count(*) FROM t GROUP BY a+1 HAVING a+1+1`.
    /// The group item `a+1` is involved in `a+1+1`, so it's a valid `HAVING`.
    /// We will check the validity by lookup this map with display name.
    ///
    /// TODO(leiysky): so far we are using `Debug` string of `Scalar` as identifier,
    /// maybe a more reasonable way is needed
    pub group_items_map: HashMap<String, usize>,
}

struct AggregateRewriter<'a> {
    pub bind_context: &'a mut BindContext,
    pub metadata: MetadataRef,
}

impl<'a> AggregateRewriter<'a> {
    pub fn new(bind_context: &'a mut BindContext, metadata: MetadataRef) -> Self {
        Self {
            bind_context,
            metadata,
        }
    }

    pub fn visit(&mut self, scalar: &Scalar) -> Result<Scalar> {
        match scalar {
            Scalar::BoundColumnRef(_) => Ok(scalar.clone()),
            Scalar::ConstantExpr(_) => Ok(scalar.clone()),
            Scalar::AndExpr(scalar) => Ok(AndExpr {
                left: Box::new(self.visit(&scalar.left)?),
                right: Box::new(self.visit(&scalar.right)?),
            }
            .into()),
            Scalar::OrExpr(scalar) => Ok(OrExpr {
                left: Box::new(self.visit(&scalar.left)?),
                right: Box::new(self.visit(&scalar.right)?),
            }
            .into()),
            Scalar::ComparisonExpr(scalar) => Ok(ComparisonExpr {
                op: scalar.op.clone(),
                left: Box::new(self.visit(&scalar.left)?),
                right: Box::new(self.visit(&scalar.right)?),
            }
            .into()),
            Scalar::FunctionCall(func) => {
                let new_args = func
                    .arguments
                    .iter()
                    .map(|arg| self.visit(arg))
                    .collect::<Result<Vec<_>>>()?;
                Ok(FunctionCall {
                    arguments: new_args,
                    func_name: func.func_name.clone(),
                    // We won't modify data type of any argument during visiting, so it's
                    // fine to reuse the types.
                    arg_types: func.arg_types.clone(),
                    return_type: func.return_type.clone(),
                }
                .into())
            }
            Scalar::Cast(cast) => Ok(CastExpr {
                argument: Box::new(self.visit(&cast.argument)?),
                from_type: cast.from_type.clone(),
                target_type: cast.target_type.clone(),
            }
            .into()),

            // TODO(leiysky): should we recursively process subquery here?
            Scalar::SubqueryExpr(_) => Ok(scalar.clone()),

            Scalar::AggregateFunction(agg_func) => self.replace_aggregate_function(agg_func),
        }
    }

    /// Replace the arguments of aggregate function with a BoundColumnRef, and
    /// add the replaced aggregate function and the arguments into `AggregateInfo`.
    fn replace_aggregate_function(&mut self, aggregate: &AggregateFunction) -> Result<Scalar> {
        let agg_info = &mut self.bind_context.aggregate_info;
        let mut replaced_args: Vec<Scalar> = Vec::with_capacity(aggregate.args.len());

        for (i, arg) in aggregate.args.iter().enumerate() {
            let name = format!("{}_arg_{}", &aggregate.func_name, i);
            let index = self
                .metadata
                .write()
                .add_column(name.clone(), arg.data_type(), None);

            // Generate a ColumnBinding for each argument of aggregates
            let column_binding = ColumnBinding {
                table_name: None,

                // TODO(leiysky): use a more reasonable name, since aggregate arguments
                // can not be referenced, the name is only for debug
                column_name: name,
                index,
                data_type: arg.data_type(),
                visible_in_unqualified_wildcard: true,
            };
            replaced_args.push(
                BoundColumnRef {
                    column: column_binding.clone(),
                }
                .into(),
            );
            agg_info.aggregate_arguments.push(ScalarItem {
                index,
                scalar: arg.clone(),
            });
        }

        let index = self.metadata.write().add_column(
            aggregate.display_name.clone(),
            aggregate.return_type.clone(),
            None,
        );

        let replaced_agg = AggregateFunction {
            display_name: aggregate.display_name.clone(),
            func_name: aggregate.func_name.clone(),
            distinct: aggregate.distinct,
            params: aggregate.params.clone(),
            args: replaced_args,
            return_type: aggregate.return_type.clone(),
        };

        agg_info.aggregate_functions.push(ScalarItem {
            scalar: replaced_agg.clone().into(),
            index,
        });
        agg_info.aggregate_functions_map.insert(
            replaced_agg.display_name.clone(),
            agg_info.aggregate_functions.len() - 1,
        );

        Ok(replaced_agg.into())
    }
}

impl<'a> Binder {
    /// Analyze aggregates in select clause, this will rewrite aggregate functions.
    /// See `AggregateRewriter` for more details.
    pub(crate) fn analyze_aggregate_select(
        &mut self,
        bind_context: &mut BindContext,
        select_list: &mut SelectList<'a>,
    ) -> Result<()> {
        for item in select_list.items.iter_mut() {
            let mut rewriter = AggregateRewriter::new(bind_context, self.metadata.clone());
            let new_scalar = rewriter.visit(&item.scalar)?;
            item.scalar = new_scalar;
        }

        Ok(())
    }

    /// Analyze aggregates in having clause, this will rewrite aggregate functions.
    /// See `AggregateRewriter` for more details.
    pub(super) async fn analyze_aggregate_having(
        &mut self,
        bind_context: &mut BindContext,
        having: &Expr<'a>,
    ) -> Result<Scalar> {
        let mut scalar_binder =
            ScalarBinder::new(bind_context, self.ctx.clone(), self.metadata.clone());
        let (scalar, _) = scalar_binder.bind(having).await?;
        let mut rewriter = AggregateRewriter::new(bind_context, self.metadata.clone());
        rewriter.visit(&scalar)
    }

    /// We have supported three kinds of `group by` items:
    ///
    ///   - Index, a integral literal, e.g. `GROUP BY 1`. It choose the 1st item in select as
    ///     group item.
    ///   - Alias, the aliased expressions specified in `SELECT` clause, e.g. column `b` in
    ///     `SELECT a as b, COUNT(a) FROM t GROUP BY b`.
    ///   - Scalar expressions that can be evaluated in current scope(doesn't contain aliases), e.g.
    ///     column `a` and expression `a+1` in `SELECT a as b, COUNT(a) FROM t GROUP BY a, a+1`.
    pub(super) async fn bind_aggregate(
        &mut self,
        bind_context: &mut BindContext,
        select_list: &SelectList<'a>,
        group_by: &[Expr<'a>],
        child: SExpr,
    ) -> Result<SExpr> {
        let mut available_aliases = vec![];

        // Extract available aliases from `SELECT` clause,
        for item in select_list.items.iter() {
            if let SelectTarget::AliasedExpr { alias: Some(_), .. } = item.select_target {
                let column =
                    self.create_column_binding(None, item.alias.clone(), item.scalar.data_type());
                available_aliases.push((column, item.scalar.clone()));
            }
        }

        self.resolve_group_items(bind_context, select_list, group_by, &available_aliases)
            .await?;

        // Enter in_grouping state
        bind_context.in_grouping = true;

        // Build a ProjectPlan, which will produce aggregate arguments and group items
        let agg_info = &bind_context.aggregate_info;
        let mut scalar_items: Vec<ScalarItem> =
            Vec::with_capacity(agg_info.aggregate_arguments.len() + agg_info.group_items.len());
        for arg in agg_info.aggregate_arguments.iter() {
            scalar_items.push(arg.clone());
        }
        for item in agg_info.group_items.iter() {
            scalar_items.push(item.clone());
        }

        let mut new_expr = child;
        if !scalar_items.is_empty() {
            let eval_scalar = EvalScalar {
                items: scalar_items,
            };
            new_expr = SExpr::create_unary(eval_scalar.into(), new_expr);
        }

        let aggregate_plan = AggregatePlan {
            group_items: bind_context.aggregate_info.group_items.clone(),
            aggregate_functions: bind_context.aggregate_info.aggregate_functions.clone(),
            from_distinct: false,
        };
        new_expr = SExpr::create_unary(aggregate_plan.into(), new_expr);

        Ok(new_expr)
    }

    pub(super) async fn bind_having(
        &mut self,
        bind_context: &BindContext,
        having: Scalar,
        child: SExpr,
    ) -> Result<SExpr> {
        let mut grouping_checker = GroupingChecker::new(bind_context);
        let scalar = grouping_checker.resolve(&having)?;

        let predicates = split_conjunctions(&scalar);

        let filter = FilterPlan {
            predicates,
            is_having: true,
        };

        Ok(SExpr::create_unary(filter.into(), child))
    }

    async fn resolve_group_items(
        &mut self,
        bind_context: &mut BindContext,
        select_list: &SelectList<'a>,
        group_by: &[Expr<'a>],
        available_aliases: &[(ColumnBinding, Scalar)],
    ) -> Result<()> {
        // Resolve group items with `FROM` context. Since the alias item can not be resolved
        // from the context, we can detect the failure and fallback to resolving with `available_aliases`.
        for expr in group_by.iter() {
            // If expr is a number literal, then this is a index group item.
            if let Expr::Literal {
                lit: Literal::Number(number_str),
                ..
            } = expr
            {
                let (scalar, alias) = Self::resolve_index_item(expr, number_str, select_list)?;
                let key = format!("{:?}", &scalar);
                if let Entry::Vacant(entry) = bind_context.aggregate_info.group_items_map.entry(key)
                {
                    // Add group item if it's not duplicated
                    let column_binding = if let Scalar::BoundColumnRef(ref column_ref) = scalar {
                        column_ref.column.clone()
                    } else {
                        self.create_column_binding(None, alias, scalar.data_type())
                    };
                    bind_context.aggregate_info.group_items.push(ScalarItem {
                        scalar,
                        index: column_binding.index,
                    });
                    entry.insert(bind_context.aggregate_info.group_items.len() - 1);
                }
                continue;
            }

            // Resolve scalar item and alias item
            let mut scalar_binder =
                ScalarBinder::new(bind_context, self.ctx.clone(), self.metadata.clone());
            let (scalar_expr, data_type) = scalar_binder
                .bind(expr)
                .await
                .or_else(|e| Self::resolve_alias_item(expr, available_aliases, e))?;

            if bind_context
                .aggregate_info
                .group_items_map
                .get(&format!("{:?}", &scalar_expr))
                .is_some()
            {
                // The group key is duplicated
                continue;
            }

            let group_item_name = format!("{:#}", expr);
            let index = if let Scalar::BoundColumnRef(BoundColumnRef {
                column: ColumnBinding { index, .. },
            }) = &scalar_expr
            {
                *index
            } else {
                self.metadata
                    .write()
                    .add_column(group_item_name.clone(), data_type.clone(), None)
            };

            bind_context.aggregate_info.group_items.push(ScalarItem {
                scalar: scalar_expr.clone(),
                index,
            });
            bind_context.aggregate_info.group_items_map.insert(
                format!("{:?}", &scalar_expr),
                bind_context.aggregate_info.group_items.len() - 1,
            );
        }
        Ok(())
    }

    fn resolve_index_item(
        expr: &Expr<'a>,
        index_string: &str,
        select_list: &SelectList<'a>,
    ) -> Result<(Scalar, String)> {
        let index = index_string.parse::<i32>().map_err(|_| {
            ErrorCode::SemanticError(
                expr.span()
                    .display_error("non-integer constant in GROUP BY".to_string()),
            )
        })?;
        // Convert to zero-based index
        let index = index - 1;
        if index < 0 || index > (select_list.items.len() as i32) {
            return Err(ErrorCode::SemanticError(expr.span().display_error(
                format!("GROUP BY position {} is not in select list", index),
            )));
        }
        let item = select_list
            .items
            .get(index as usize)
            .ok_or_else(|| ErrorCode::LogicalError("Should not fail"))?;

        let scalar = item.scalar.clone();
        let alias = item.alias.clone();

        Ok((scalar, alias))
    }

    fn resolve_alias_item(
        expr: &Expr<'a>,
        available_aliases: &[(ColumnBinding, Scalar)],
        original_error: ErrorCode,
    ) -> Result<(Scalar, DataTypeImpl)> {
        let mut result: Vec<Scalar> = vec![];
        // If cannot resolve group item, then try to find an available alias
        for (column_binding, scalar) in available_aliases.iter() {
            let col_name = column_binding.column_name.as_str();
            // TODO(leiysky): check if expr is a qualified name
            if let Expr::ColumnRef { column, .. } = expr {
                if col_name == column.name.to_lowercase().as_str() {
                    result.push(scalar.clone());
                }
            }
        }

        if result.is_empty() {
            Err(original_error)
        } else if result.len() > 1 {
            Err(ErrorCode::SemanticError(expr.span().display_error(
                format!("GROUP BY \"{}\" is ambiguous", expr),
            )))
        } else {
            Ok((result[0].clone(), result[0].data_type()))
        }
    }
}
