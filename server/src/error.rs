use std::{error::Error, fmt::Display};

use axum::{Json, http::StatusCode, response::IntoResponse};
use serde::Serialize;

#[derive(Debug)]
pub struct AppError {
    inner: Box<dyn Error + Send + 'static>,
}

impl Display for AppError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.inner)
    }
}

impl<E: Error + Send + 'static> From<E> for AppError {
    fn from(value: E) -> Self {
        Self {
            inner: Box::new(value),
        }
    }
}

#[derive(Serialize)]
struct ErrorBody {
    error: String,
}
impl IntoResponse for AppError {
    fn into_response(self) -> axum::response::Response {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorBody {
                error: self.inner.to_string(),
            }),
        )
            .into_response()
    }
}

pub type AppResult<T> = Result<T, AppError>;
