CREATE USER cortex_service_user
  PASSWORD = 'StrongP@ssw0rd!'
  DEFAULT_ROLE = cortex_user_role
  MUST_CHANGE_PASSWORD = FALSE
  COMMENT = 'Service user for Cortex MCP integration';
