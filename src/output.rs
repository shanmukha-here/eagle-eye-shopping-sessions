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
const USER: &str = "message user_daily {
    REQUIRED BINARY user_id (UTF8); REQUIRED INT32 day (DATE);
    REQUIRED INT64 sessions; REQUIRED INT64 events; REQUIRED INT64 revenue_cents;
    OPTIONAL BINARY top_category (UTF8); }";
const CAMPAIGN: &str = "message campaign_daily {
    REQUIRED BINARY campaign_id (UTF8); REQUIRED BINARY channel (UTF8); REQUIRED INT32 day (DATE);
    REQUIRED INT64 sessions; REQUIRED INT64 purchasing_sessions; REQUIRED INT64 revenue_cents;
    REQUIRED INT64 assisted_sessions; }";

pub fn users(path: &Path, rows: &[UserRow], keys: &Keys, categories: &[ByteArray]) {
    let mut f = file(path, USER);
    for batch in rows.chunks(32768) {
        let mut group = f.next_row_group().unwrap();
        let mut prev = None;
        let mut value = ByteArray::default();
        let names: Vec<_> = batch
            .iter()
            .map(|r| {
                if prev != Some(r.u) {
                    prev = Some(r.u);
                    value = ByteArray::from(keys.bytes(r.u));
                }
                value.clone()
            })
            .collect();
        column::<ByteArrayType>(&mut group, &names, None);
        column::<Int32Type>(
            &mut group,
            &batch.iter().map(|r| r.day).collect::<Vec<_>>(),
            None,
        );
        column::<Int64Type>(
            &mut group,
            &batch.iter().map(|r| r.sessions).collect::<Vec<_>>(),
            None,
        );
        column::<Int64Type>(
            &mut group,
            &batch.iter().map(|r| r.events).collect::<Vec<_>>(),
            None,
        );
        column::<Int64Type>(
            &mut group,
            &batch.iter().map(|r| r.revenue).collect::<Vec<_>>(),
            None,
        );
        let levels: Vec<_> = batch.iter().map(|r| i16::from(r.top.is_some())).collect();
        let cats: Vec<_> = batch
            .iter()
            .filter_map(|r| r.top.map(|c| categories[c as usize].clone()))
            .collect();
        column::<ByteArrayType>(&mut group, &cats, Some(&levels));
        group.close().unwrap();
    }
    f.close().unwrap();
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
