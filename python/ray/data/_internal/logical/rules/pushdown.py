class PushdownCapabilities:  
    """定义 Datasource 的下推能力"""  
      
    def supports_filter_pushdown(self) -> bool:  
        """是否支持过滤器下推"""  
        return False  
      
    def supports_projection_pushdown(self) -> bool:  
        """是否支持投影下推"""  
        return False  
      
    def supports_limit_pushdown(self) -> bool:  
        """是否支持限制下推"""  
        return False  
      
    def apply_pushdowns(self, pushdowns: Dict[str, Any]) -> None:  
        """应用下推优化"""  
        pass  