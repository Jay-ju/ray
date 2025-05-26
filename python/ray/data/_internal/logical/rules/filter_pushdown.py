import copy  
from typing import Dict, List, Optional, Set, Any  
from ray.data._internal.logical.interfaces import LogicalOperator, LogicalPlan, Rule  
from ray.data._internal.logical.operators.read_operator import Read  
from ray.data._internal.logical.operators.map_operator import Filter  
import pyarrow.compute as pc  
  
  
class FilterPushdownRule(Rule):  
    """实现Filter下推优化的规则"""  
      
    def apply(self, plan: LogicalPlan) -> LogicalPlan:  
        optimized_dag = self._push_filters(plan.dag)  
        return LogicalPlan(dag=optimized_dag, context=plan.context)  
  
    def _push_filters(self, root: LogicalOperator) -> LogicalOperator:  
        """后序遍历处理所有Filter算子"""  
        # 先处理子节点  
        if hasattr(root, 'input_dependencies') and root.input_dependencies:  
            processed_children = []  
            for child in root.input_dependencies:  
                processed_children.append(self._push_filters(child))  
            root._input_dependencies = processed_children  
          
        # 如果当前节点是Filter，尝试下推  
        if isinstance(root, Filter):  
            return self._push_filter_operator(root)  
        return root  
  
    def _push_filter_operator(self, filter_op: Filter) -> LogicalOperator:  
        """处理Filter算子的下推逻辑"""  
        if not hasattr(filter_op, 'input_dependencies') or not filter_op.input_dependencies:  
            return filter_op  
              
        child = filter_op.input_dependencies[0]  
          
        # 只有当filter有表达式时才尝试下推  
        if not hasattr(filter_op, 'filter_expr') or filter_op.filter_expr is None:  
            return filter_op  
              
        predicate = filter_op.filter_expr  
  
        # 尝试下推到Read算子  
        if isinstance(child, Read):  
            return self._push_into_read(child, filter_op, predicate)  
          
        # 无法下推则保持原状  
        return filter_op  
  
    def _push_into_read(self, read_op: Read, filter_op: Filter, predicate) -> LogicalOperator:  
        """下推Filter到Read算子"""  
        # 检查数据源是否支持过滤器下推  
        if not self._supports_filter_pushdown(read_op):  
            return filter_op  
              
        # 创建新的Read算子副本  
        new_read = copy.deepcopy(read_op)  
          
        # 确保pushdowns属性存在  
        if not hasattr(new_read, 'pushdowns'):  
            new_read.pushdowns = {}  
          
        # 添加过滤器到pushdowns  
        existing_filters = new_read.pushdowns.get("filters", [])  
        if isinstance(existing_filters, list):  
            new_read.pushdowns["filters"] = existing_filters + [predicate]  
        else:  
            new_read.pushdowns["filters"] = [predicate]  
          
        # 返回新的Read算子，不再需要Filter算子  
        return new_read  
  
    def _supports_filter_pushdown(self, read_op: Read) -> bool:  
        """检查Read算子是否支持过滤器下推"""  
        # 检查数据源类型  
        if hasattr(read_op, 'datasource'):  
            datasource_name = read_op.datasource.__class__.__name__  
            # 支持过滤器下推的数据源列表  
            supported_sources = [  
                'ParquetDatasource',  
                'IcebergDatasource',   
                'ClickHouseDatasource',  
                'CSVDatasource',  
                'JSONDatasource'  
            ]  
            return datasource_name in supported_sources  
        return False  
  
    def _can_push_predicate(self, predicate, read_op: Read) -> bool:  
        """检查谓词是否可以下推到特定的Read算子"""  
        try:  
            # 获取谓词中引用的列  
            referenced_cols = self._get_referenced_columns(predicate)  
              
            # 检查Read算子的schema是否包含这些列  
            if hasattr(read_op, 'schema') and read_op.schema:  
                available_cols = set(read_op.schema.names)  
                return referenced_cols.issubset(available_cols)  
              
            # 如果无法确定schema，保守地允许下推  
            return True  
        except Exception:  
            # 如果分析失败，不进行下推  
            return False  
  
    def _get_referenced_columns(self, predicate) -> Set[str]:  
        """获取谓词中引用的所有列名"""  
        cols = set()  
          
        def _extract_columns(expr):  
            if hasattr(expr, '_name'):  
                cols.add(expr._name)  
            elif hasattr(expr, 'operands'):  
                for operand in expr.operands:  
                    _extract_columns(operand)  
            elif hasattr(expr, 'left') and hasattr(expr, 'right'):  
                _extract_columns(expr.left)  
                _extract_columns(expr.right)  
          
        try:  
            _extract_columns(predicate)  
        except Exception:  
            pass  
              
        return cols  
  
  
# 将规则注册到优化器中  
def register_filter_pushdown_rule():  
    """注册Filter下推规则到Ray Data的优化器中"""  
    try:  
        from ray.data._internal.logical.optimizers import get_logical_optimizer  
        from ray.data._internal.logical.ruleset import LogicalRuleset  
          
        # 获取当前的规则集  
        optimizer = get_logical_optimizer()  
        if hasattr(optimizer, '_ruleset'):  
            ruleset = optimizer._ruleset  
        else:  
            ruleset = LogicalRuleset()  
          
        # 添加Filter下推规则  
        filter_pushdown_rule = FilterPushdownRule()  
        if hasattr(ruleset, 'add_rule'):  
            ruleset.add_rule(filter_pushdown_rule)  
        elif hasattr(ruleset, '_rules'):  
            ruleset._rules.append(filter_pushdown_rule)  
              
    except ImportError:  
        print("Warning: Could not register filter pushdown rule - Ray Data internal APIs not available")  
  
  
# 使用示例  
if __name__ == "__main__":  
    # 注册规则  
    register_filter_pushdown_rule()  
      
    # 创建测试数据集  
    import ray  
      
    # 创建一个简单的数据集并应用过滤器  
    ds = ray.data.range(1000)  
      
    # 应用过滤器 - 这将触发我们的下推规则  
    filtered_ds = ds.filter(expr="id > 500")  
      
    # 执行并查看结果  
    result = filtered_ds.take(5)  
    print("Filter pushdown test result:", result)  
      
    # 查看执行计划  
    print("Execution plan:")  
    print(filtered_ds._plan.get_plan_as_string(type(filtered_ds)))