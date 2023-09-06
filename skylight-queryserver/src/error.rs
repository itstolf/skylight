pub enum Error {
    Status(axum::http::StatusCode, Option<String>),
    BoxError(axum::BoxError),
}

impl Error {
    pub fn status(code: axum::http::StatusCode) -> Self {
        Self::Status(code, None)
    }
}

impl axum::response::IntoResponse for Error {
    fn into_response(self) -> axum::response::Response {
        match self {
            Error::Status(code, reason) => (
                code,
                reason
                    .or_else(|| code.canonical_reason().map(|v| v.to_string()))
                    .unwrap_or_else(|| code.to_string()),
            ),
            Error::BoxError(err) => (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("{}", err),
            ),
        }
        .into_response()
    }
}

impl<E> From<E> for Error
where
    E: Into<axum::BoxError>,
{
    fn from(err: E) -> Self {
        Self::BoxError(err.into())
    }
}
