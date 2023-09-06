pub struct Query<T>(pub T);

#[async_trait::async_trait]
impl<T, S> axum::extract::FromRequestParts<S> for Query<T>
where
    T: serde::de::DeserializeOwned,
    S: Send + Sync,
{
    type Rejection = Rejection;

    async fn from_request_parts(
        parts: &mut http::request::Parts,
        _state: &S,
    ) -> Result<Self, Self::Rejection> {
        Self::try_from_uri(&parts.uri)
    }
}

impl<T> Query<T>
where
    T: serde::de::DeserializeOwned,
{
    pub fn try_from_uri(value: &hyper::Uri) -> Result<Self, Rejection> {
        let query = value.query().unwrap_or_default();
        let params = serde_querystring::from_str(query, serde_querystring::ParseMode::Duplicate)
            .map_err(|e| Rejection { error: e })?;
        Ok(Query(params))
    }
}

#[derive(Debug)]
pub struct Rejection {
    error: serde_querystring::Error,
}

impl std::fmt::Display for Rejection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.error)
    }
}

impl axum::response::IntoResponse for Rejection {
    fn into_response(self) -> axum::response::Response {
        (axum::http::StatusCode::BAD_REQUEST, self.to_string()).into_response()
    }
}

impl std::error::Error for Rejection {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.error)
    }
}
