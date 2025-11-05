-- ACCOUNTADMIN権限で実行
USE ROLE ACCOUNTADMIN;

-- OAuth Security Integrationを作成
CREATE OR REPLACE SECURITY INTEGRATION git_oauth_integration
  TYPE = oauth
  ENABLED = TRUE
  OAUTH_CLIENT = custom
  OAUTH_CLIENT_TYPE = 'CONFIDENTIAL'
  OAUTH_REDIRECT_URI = 'https://AIKMQLP-NK59420.snowflakecomputing.com/oauth/authorization-callback'
  OAUTH_ISSUE_REFRESH_TOKENS = TRUE
  OAUTH_REFRESH_TOKEN_VALIDITY = 7776000  -- 90日
  BLOCKED_ROLES_LIST = ();


-- OAuth用のSecretを作成
CREATE OR REPLACE SECRET git_oauth_secret
  TYPE = oauth2
  OAUTH_SCOPES = ('repo')
  OAUTH_TOKEN_ENDPOINT = 'https://github.com/login/oauth/access_token'
  OAUTH_AUTHORIZATION_ENDPOINT = 'https://github.com/login/oauth/authorize'
  OAUTH_CLIENT_ID = 'Iv1.Ov23licCs9O4Y7DgfALV'  -- ステップ2でコピーしたClient ID
  OAUTH_CLIENT_SECRET = '31d05b321164585129284c95ebb63fd08efb2155';  -- ステップ2でコピーしたClient Secret

  SHOW API INTEGRATIONS;

-- OAuth対応のAPI Integrationを作成
CREATE OR REPLACE API INTEGRATION git_api_integration_oauth
  API_PROVIDER = git_https_api
  API_ALLOWED_PREFIXES = ('https://github.com/nadayoshi523/')
  ALLOWED_AUTHENTICATION_SECRETS = (git_oauth_secret)
  ENABLED = TRUE;

  CREATE OR REPLACE API INTEGRATION git_api_integration_oauth
  API_PROVIDER = git_https_api
  API_ALLOWED_PREFIXES = ('https://github.com/')
  ALLOWED_AUTHENTICATION_SECRETS = git_oauth_secret
  ENABLED = TRUE;

  -- 既存のAPI Integrationを確認
SHOW API INTEGRATIONS;
CREATE OR REPLACE API INTEGRATION git_api_integration_oauth
  API_PROVIDER = git_https_api
  API_ALLOWED_PREFIXES = ('https://github.com/nadayoshi523/')
  ALLOWED_AUTHENTICATION_SECRETS = (git_oauth_secret)
  ENABLED = TRUE;