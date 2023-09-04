pub enum Error {
    Status(axum::http::StatusCode, String),
    Anyhow(anyhow::Error),
}

impl axum::response::IntoResponse for Error {
    fn into_response(self) -> axum::response::Response {
        match self {
            Error::Status(code, reason) => (code, reason),
            Error::Anyhow(err) => (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("{}", err),
            ),
        }
        .into_response()
    }
}

impl<E> From<E> for Error
where
    E: Into<anyhow::Error>,
{
    fn from(err: E) -> Self {
        Self::Anyhow(err.into())
    }
}
