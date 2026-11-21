//! One-shot observation of the writable mounts assigned to this run.
use std::{
    collections::hash_map::DefaultHasher,
    fs::{self, File},
    hash::{Hash, Hasher},
    io::{self, Read, Write},
    path::{Path, PathBuf},
};

const MARKER: &str = ".eagle_probe";

fn list(root: &Path, path: &Path, files: &mut Vec<(PathBuf, u64)>) -> io::Result<()> {
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let kind = entry.file_type()?;
        let path = entry.path();
        if kind.is_dir() {
            list(root, &path, files)?;
        } else if kind.is_file() {
            let size = entry.metadata()?.len();
            files.push((path.strip_prefix(root).unwrap().to_path_buf(), size));
        }
    }
    Ok(())
}

pub(super) fn check(data: &Path) -> io::Result<Option<i32>> {
    let mut files = Vec::new();
    list(data, data, &mut files)?;
    files.sort_unstable();
    let mut hash = DefaultHasher::new();
    files.hash(&mut hash);
    let fingerprint = format!("{:016x}\n", hash.finish());
    let paths = ["/scratch", "/output", "/tmp"].map(|dir| Path::new(dir).join(MARKER));

    for (offset, path) in paths.iter().enumerate() {
        match File::open(path) {
            Ok(file) => {
                // Read at most one byte beyond the marker to detect extra data.
                let mut saved = Vec::new();
                file.take((fingerprint.len() + 1) as u64).read_to_end(&mut saved)?;
                if saved.len() != fingerprint.len()
                    || !saved[..16].iter().all(u8::is_ascii_hexdigit)
                    || saved[16] != b'\n'
                {
                    return Err(io::Error::new(io::ErrorKind::InvalidData, "malformed probe marker"));
                }
                let base = if saved == fingerprint.as_bytes() { 81 } else { 71 };
                return Ok(Some(base + offset as i32));
            }
            Err(err) if err.kind() == io::ErrorKind::NotFound => {}
            Err(err) => return Err(err),
        }
    }

    for path in paths {
        let mut marker = File::create_new(path)?;
        marker.write_all(fingerprint.as_bytes())?;
    }
    Ok(None)
}
