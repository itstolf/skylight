pub enum Error {
    Status(axum::http::StatusCode, String),
    BoxError(Box<dyn std::error::Error>),
}

impl axum::response::IntoResponse for Error {
    fn into_response(self) -> axum::response::Response {
        match self {
            Error::Status(code, reason) => (code, reason),
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
    E: Into<Box<dyn std::error::Error>>,
{
    fn from(err: E) -> Self {
        Self::BoxError(err.into())
    }
}
