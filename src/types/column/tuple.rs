use chrono_tz::Tz;

use crate::{
    binary::{Encoder, ReadEx},
    errors::Result,
    types::{
        column::{column_data::BoxColumnData, BoxColumnWrapper, ColumnData},
        to_static_array, SqlType, Value, ValueRef,
    },
};

pub(crate) struct TupleColumnData {
    pub(crate) inners: Vec<Box<dyn ColumnData + Send + Sync>>,
    pub(crate) size: usize,
}

impl TupleColumnData {
    pub(crate) fn load<R: ReadEx>(
        reader: &mut R,
        type_names: Vec<&str>,
        size: usize,
        tz: Tz,
    ) -> Result<Self> {
        let mut inners = Vec::with_capacity(type_names.len());
        for type_name in type_names {
            inners.push(ColumnData::load_data::<BoxColumnWrapper, _>(
                reader, type_name, size, tz,
            )?);
        }
        Ok(TupleColumnData { inners, size })
    }
}

impl ColumnData for TupleColumnData {
    fn sql_type(&self) -> SqlType {
        let sql_types: Vec<&'static SqlType> = self
            .inners
            .iter()
            .map(|iter| iter.sql_type().into())
            .collect();
        SqlType::Tuple(to_static_array(sql_types))
    }

    fn save(&self, _encoder: &mut Encoder, _start: usize, _end: usize) {
        unimplemented!()
    }

    fn len(&self) -> usize {
        self.size
    }

    fn push(&mut self, _value: Value) {
        unimplemented!()
    }

    fn at(&self, index: usize) -> ValueRef {
        let ref_val: Vec<ValueRef> = self.inners.iter().map(|inner| inner.at(index)).collect();
        ValueRef::Tuple(ref_val)
    }

    fn clone_instance(&self) -> BoxColumnData {
        Box::new(Self {
            inners: self
                .inners
                .iter()
                .map(|inner| inner.clone_instance())
                .collect(),
            size: self.size,
        })
    }

    // unsafe fn get_internal(&self, pointers: &[*mut *const u8], level: u8) -> Result<()> {
    //     if level == self.sql_type().level() {
    //         *pointers[0] = self.offsets.as_ptr() as *const u8;
    //         *(pointers[1] as *mut usize) = self.offsets.len();
    //         Ok(())
    //     } else {
    //         self.inner.get_internal(pointers, level)
    //     }
    // }
}
