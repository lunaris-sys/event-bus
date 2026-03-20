use crate::proto::Event;

/// Validate that a decoded event has all required fields populated.
pub fn validate(event: &Event) -> Result<(), ValidationError> {
    if event.id.is_empty() {
        return Err(ValidationError::MissingField("id"));
    }
    if event.r#type.is_empty() {
        return Err(ValidationError::MissingField("type"));
    }
    if event.source.is_empty() {
        return Err(ValidationError::MissingField("source"));
    }
    if event.timestamp == 0 {
        return Err(ValidationError::MissingField("timestamp"));
    }
    if event.session_id.is_empty() {
        return Err(ValidationError::MissingField("session_id"));
    }
    Ok(())
}

#[derive(Debug)]
pub enum ValidationError {
    MissingField(&'static str),
}

impl std::fmt::Display for ValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ValidationError::MissingField(field) => write!(f, "missing required field: {field}"),
        }
    }
}

impl std::error::Error for ValidationError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_event() -> Event {
        Event {
            id: "01950000-0000-7000-8000-000000000001".to_string(),
            r#type: "file.opened".to_string(),
            timestamp: 1_000_000,
            source: "ebpf".to_string(),
            pid: 1234,
            session_id: "session-abc".to_string(),
            payload: vec![],
        }
    }

    #[test]
    fn valid_event_passes() {
        assert!(validate(&valid_event()).is_ok());
    }

    #[test]
    fn missing_id_fails() {
        let mut e = valid_event();
        e.id = String::new();
        assert!(validate(&e).is_err());
    }

    #[test]
    fn missing_type_fails() {
        let mut e = valid_event();
        e.r#type = String::new();
        assert!(validate(&e).is_err());
    }

    #[test]
    fn missing_source_fails() {
        let mut e = valid_event();
        e.source = String::new();
        assert!(validate(&e).is_err());
    }

    #[test]
    fn zero_timestamp_fails() {
        let mut e = valid_event();
        e.timestamp = 0;
        assert!(validate(&e).is_err());
    }
}
