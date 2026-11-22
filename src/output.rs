use crate::{
    aggregate::{Stats, UserRow},
    keys::Keys,
};
use parquet::{
    data_type::{ByteArray, ByteArrayType, DataType, Int32Type, Int64Type},
    file::{
        properties::{EnabledStatistics, WriterProperties},
        writer::{SerializedFileWriter, SerializedRowGroupWriter},
    },
    schema::parser::parse_message_type,
};
use std::{collections::BTreeMap, fs::File, path::Path, sync::Arc};

fn file(path: &Path, schema: &str) -> SerializedFileWriter<File> {
    let props = WriterProperties::builder()
        .set_dictionary_enabled(false)
        .set_statistics_enabled(EnabledStatistics::None)
        .build();
    SerializedFileWriter::new(
        File::create(path).unwrap(),
        Arc::new(parse_message_type(schema).unwrap()),
        Arc::new(props),
    )
    .unwrap()
}
fn column<T: DataType>(
    group: &mut SerializedRowGroupWriter<File>,
    values: &[T::T],
    levels: Option<&[i16]>,
) {
    let mut col = group.next_column().unwrap().unwrap();
    col.typed::<T>().write_batch(values, levels, None).unwrap();
    col.close().unwrap();
}
const CAMPAIGN: &str = "message campaign_daily {
    REQUIRED BINARY campaign_id (UTF8); REQUIRED BINARY channel (UTF8); REQUIRED INT32 day (DATE);
    REQUIRED INT64 sessions; REQUIRED INT64 purchasing_sessions; REQUIRED INT64 revenue_cents;
    REQUIRED INT64 assisted_sessions; }";

pub(crate) struct UserWriter {
    output: crate::pages::Output<6>,
    name: Vec<u8>,
    previous: Option<u64>,
    categories: Vec<ByteArray>,
}

impl UserWriter {
    pub(crate) fn open(path: &Path, categories: &[ByteArray], narrow: bool, dictionary: bool) -> Self {
        Self {
            output: crate::pages::Output::users(path, narrow, dictionary),
            name: Vec::new(),
            previous: None,
            categories: categories.to_vec(),
        }
    }

    pub(crate) fn row(&mut self, row: UserRow, keys: &Keys) {
        let same = self.previous == Some(row.u);
        if !same {
            self.name.clear();
            keys.append_bytes(row.u, &mut self.name);
            self.previous = Some(row.u);
        }
        self.output.user(&self.name, same, row.day, row.sessions, row.events, row.revenue,
            row.top.map(|c| self.categories[c as usize].data()));
    }

    pub(crate) fn close(self) -> usize {
        self.output.close()
    }
}

pub fn users(path: &Path, rows: &[UserRow], keys: &Keys, categories: &[ByteArray]) {
    let mut writer = UserWriter::open(path, categories, false, false);
    for row in rows {
        writer.row(*row, keys);
    }
    writer.close();
}
pub fn campaigns(path: &Path, days: BTreeMap<i32, Vec<Stats>>, catalog: &[(ByteArray, ByteArray)]) {
    let mut f = file(path, CAMPAIGN);
    for (day, all) in days {
        let records: Vec<_> = all
            .into_iter()
            .enumerate()
            .filter(|(_, r)| r.sessions != 0 || r.assists != 0)
            .collect();
        if records.is_empty() {
            continue;
        }
        let mut group = f.next_row_group().unwrap();
        column::<ByteArrayType>(
            &mut group,
            &records
                .iter()
                .map(|(i, _)| catalog[*i].0.clone())
                .collect::<Vec<_>>(),
            None,
        );
        column::<ByteArrayType>(
            &mut group,
            &records
                .iter()
                .map(|(i, _)| catalog[*i].1.clone())
                .collect::<Vec<_>>(),
            None,
        );
        column::<Int32Type>(&mut group, &vec![day; records.len()], None);
        column::<Int64Type>(
            &mut group,
            &records.iter().map(|(_, r)| r.sessions).collect::<Vec<_>>(),
            None,
        );
        column::<Int64Type>(
            &mut group,
            &records.iter().map(|(_, r)| r.buying).collect::<Vec<_>>(),
            None,
        );
        column::<Int64Type>(
            &mut group,
            &records.iter().map(|(_, r)| r.revenue).collect::<Vec<_>>(),
            None,
        );
        column::<Int64Type>(
            &mut group,
            &records.iter().map(|(_, r)| r.assists).collect::<Vec<_>>(),
            None,
        );
        group.close().unwrap();
    }
    f.close().unwrap();
}
