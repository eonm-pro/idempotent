use std::fs::File;
use std::io::{self, stdin, BufRead, BufReader};
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub enum Input {
    Stdin,
    File(PathBuf),
}

impl Input {
    pub fn buf_reader(&self) -> Result<Box<dyn BufRead>, io::Error> {
        match self {
            Input::Stdin => {
                let stdin = stdin().lock();

                Ok(Box::new(BufReader::new(stdin)))
            }
            Input::File(path) => Ok(Box::new(BufReader::new(File::open(path)?))),
        }
    }
}
