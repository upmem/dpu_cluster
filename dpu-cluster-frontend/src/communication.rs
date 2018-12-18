use std::net::TcpStream;
use std::io::Write;
use std::io::Read;
use std::io;
use serde::de::DeserializeOwned;

#[derive(Serialize, Deserialize)]
pub enum Command {
    Register,
    Unregister,
    Configuration,
    Allocate,
    Free,
}

pub trait Request<Response: DeserializeOwned> {
    fn command(&self) -> Command;
}

pub struct ClusterConnection {
    stream: TcpStream
}

pub enum CommunicationError {
    SerializationError(serde_json::Error),
    TcpError(io::Error),
}

impl From<serde_json::Error> for CommunicationError {
    fn from(err: serde_json::Error) -> Self {
        CommunicationError::SerializationError(err)
    }
}

impl From<io::Error> for CommunicationError {
    fn from(err: io::Error) -> Self {
        CommunicationError::TcpError(err)
    }
}

impl ClusterConnection {
    pub fn send<'a, Req, Res>(&mut self, request: &Req) -> Result<Res, CommunicationError>
        where Req: Request<Res>,
              Res: DeserializeOwned
    {
        let command = request.command();
        let input = serde_json::to_string(&command)?;
        self.stream.write(input.as_bytes())?;

        let mut output = String::default();
        self.stream.read_to_string(&mut output)?;
        let result = serde_json::from_str(&output)?;

        Ok(result)
    }
}

pub struct RegisterRequest;

#[derive(Serialize, Deserialize)]
pub struct RegisterResponse(u32);

impl Request<RegisterResponse> for RegisterRequest {
    fn command(&self) -> Command {
        Command::Register
    }
}