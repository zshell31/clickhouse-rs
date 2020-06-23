use chrono::prelude::*;
use chrono_tz::Tz;
use std::net::{Ipv4Addr, Ipv6Addr};

use crate::types::{Enum16, Enum8};
use crate::{
    errors::{Error, FromSqlError, Result},
    types::{column::{Either, datetime64::to_datetime}, Decimal, SqlType, ValueRef},
};

pub type FromSqlResult<T> = Result<T>;

pub trait FromSql<'a>: Sized {
    fn from_sql(value: ValueRef<'a>) -> FromSqlResult<Self>;
}

macro_rules! from_sql_impl {
    ( $( $t:ident: $k:ident ),* ) => {
        $(
            impl<'a> FromSql<'a> for $t {
                fn from_sql(value: ValueRef<'a>) -> FromSqlResult<Self> {
                    match value {
                        ValueRef::$k(v) => Ok(v),
                        _ => {
                            let from = SqlType::from(value.clone()).to_string();
                            Err(Error::FromSql(FromSqlError::InvalidType { src: from, dst: stringify!($t).into() }))
                        }
                    }
                }
            }
        )*
    };
}

impl<'a> FromSql<'a> for Decimal {
    fn from_sql(value: ValueRef<'a>) -> FromSqlResult<Self> {
        match value {
            ValueRef::Decimal(v) => Ok(v),
            _ => {
                let from = SqlType::from(value.clone()).to_string();
                Err(Error::FromSql(FromSqlError::InvalidType {
                    src: from,
                    dst: "Decimal".into(),
                }))
            }
        }
    }
}

impl<'a> FromSql<'a> for Enum8 {
    fn from_sql(value: ValueRef<'a>) -> FromSqlResult<Self> {
        match value {
            ValueRef::Enum8(_enum_values, v) => Ok(v),
            _ => {
                let from = SqlType::from(value.clone()).to_string();

                Err(Error::FromSql(FromSqlError::InvalidType {
                    src: from,
                    dst: "Enum8".into(),
                }))
            }
        }
    }
}

impl<'a> FromSql<'a> for Enum16 {
    fn from_sql(value: ValueRef<'a>) -> FromSqlResult<Self> {
        match value {
            ValueRef::Enum16(_enum_values, v) => Ok(v),
            _ => {
                let from = SqlType::from(value.clone()).to_string();

                Err(Error::FromSql(FromSqlError::InvalidType {
                    src: from,
                    dst: "Enum16".into(),
                }))
            }
        }
    }
}

impl<'a> FromSql<'a> for &'a str {
    fn from_sql(value: ValueRef<'a>) -> FromSqlResult<&'a str> {
        value.as_str()
    }
}

impl<'a> FromSql<'a> for &'a [u8] {
    fn from_sql(value: ValueRef<'a>) -> FromSqlResult<&'a [u8]> {
        value.as_bytes()
    }
}

impl<'a> FromSql<'a> for String {
    fn from_sql(value: ValueRef<'a>) -> FromSqlResult<Self> {
        value.as_str().map(str::to_string)
    }
}

impl<'a> FromSql<'a> for Ipv4Addr {
    fn from_sql(value: ValueRef<'a>) -> FromSqlResult<Self> {
        match value {
            ValueRef::Ipv4(ip) => Ok(Ipv4Addr::from(ip)),
            _ => {
                let from = SqlType::from(value.clone()).to_string();
                Err(Error::FromSql(FromSqlError::InvalidType {
                    src: from,
                    dst: "Ipv4".into(),
                }))
            }
        }
    }
}

impl<'a> FromSql<'a> for Ipv6Addr {
    fn from_sql(value: ValueRef<'a>) -> FromSqlResult<Self> {
        match value {
            ValueRef::Ipv6(ip) => Ok(Ipv6Addr::from(ip)),
            _ => {
                let from = SqlType::from(value.clone()).to_string();
                Err(Error::FromSql(FromSqlError::InvalidType {
                    src: from,
                    dst: "Ipv6".into(),
                }))
            }
        }
    }
}

impl<'a> FromSql<'a> for uuid::Uuid {
    fn from_sql(value: ValueRef<'a>) -> FromSqlResult<Self> {
        match value {
            ValueRef::Uuid(row) => Ok(uuid::Uuid::from_bytes(row)),
            _ => {
                let from = SqlType::from(value.clone()).to_string();
                Err(Error::FromSql(FromSqlError::InvalidType {
                    src: from,
                    dst: "Uuid".into(),
                }))
            }
        }
    }
}

macro_rules! from_sql_vec_impl {
    ( $( $t:ty: $k:pat => $f:expr ),* ) => {
        $(
            impl<'a> FromSql<'a> for Vec<$t> {
                fn from_sql(value: ValueRef<'a>) -> FromSqlResult<Self> {
                    match value {
                        ValueRef::Array($k, vs) => {
                            let f: fn(ValueRef<'a>) -> FromSqlResult<$t> = $f;
                            let mut result = Vec::with_capacity(vs.len());
                            for v in vs.iter() {
                                let value: $t = f(v.clone())?;
                                result.push(value);
                            }
                            Ok(result)
                        }
                        _ => {
                            let from = SqlType::from(value.clone()).to_string();
                            Err(Error::FromSql(FromSqlError::InvalidType {
                                src: from,
                                dst: format!("Vec<{}>", stringify!($t)).into(),
                            }))
                        }
                    }
                }
            }
        )*
    };
}

from_sql_vec_impl! {
    &'a str: SqlType::String => |v| v.as_str(),
    String: SqlType::String => |v| v.as_string(),
    Date<Tz>: SqlType::Date => |z| Ok(z.into()),
    DateTime<Tz>: SqlType::DateTime(_) => |z| Ok(z.into())
}

impl<'a> FromSql<'a> for Vec<u8> {
    fn from_sql(value: ValueRef<'a>) -> FromSqlResult<Self> {
        match value {
            ValueRef::Array(SqlType::UInt8, vs) => {
                let mut result = Vec::with_capacity(vs.len());
                for v in vs.iter() {
                    result.push(v.clone().into());
                }
                Ok(result)
            }
            _ => value.as_bytes().map(|bs| bs.to_vec()),
        }
    }
}

macro_rules! from_sql_vec_impl {
    ( $( $t:ident: $k:ident ),* ) => {
        $(
            impl<'a> FromSql<'a> for Vec<$t> {
                fn from_sql(value: ValueRef<'a>) -> Result<Self> {
                    match value {
                        ValueRef::Array(SqlType::$k, vs) => {
                            let mut result = Vec::with_capacity(vs.len());
                            for v in vs.iter() {
                                let val: $t = v.clone().into();
                                result.push(val);
                            }
                            Ok(result)
                        }
                        _ => {
                            let from = SqlType::from(value.clone()).to_string();
                            Err(Error::FromSql(FromSqlError::InvalidType { src: from, dst: stringify!($t).into() }))
                        }
                    }
                }
            }
        )*
    };
}

from_sql_vec_impl! {
    i8: Int8,
    i16: Int16,
    i32: Int32,
    i64: Int64,

    u16: UInt16,
    u32: UInt32,
    u64: UInt64,

    f32: Float32,
    f64: Float64
}

impl<'a, T> FromSql<'a> for Option<T>
    where
        T: FromSql<'a>,
{
    fn from_sql(value: ValueRef<'a>) -> FromSqlResult<Self> {
        match value {
            ValueRef::Nullable(e) => match e {
                Either::Left(_) => Ok(None),
                Either::Right(u) => {
                    let value_ref = u.as_ref().clone();
                    Ok(Some(T::from_sql(value_ref)?))
                }
            },
            _ => {
                let from = SqlType::from(value.clone()).to_string();
                Err(Error::FromSql(FromSqlError::InvalidType {
                    src: from,
                    dst: stringify!($t).into(),
                }))
            }
        }
    }
}

impl<'a> FromSql<'a> for Date<Tz> {
    fn from_sql(value: ValueRef<'a>) -> FromSqlResult<Self> {
        match value {
            ValueRef::Date(v, tz) => {
                let time = tz.timestamp(i64::from(v) * 24 * 3600, 0);
                Ok(time.date())
            }
            _ => {
                let from = SqlType::from(value).to_string();
                Err(Error::FromSql(FromSqlError::InvalidType {
                    src: from,
                    dst: "Date<Tz>".into(),
                }))
            }
        }
    }
}

impl<'a> FromSql<'a> for DateTime<Tz> {
    fn from_sql(value: ValueRef<'a>) -> FromSqlResult<Self> {
        match value {
            ValueRef::DateTime(v, tz) => {
                let time = tz.timestamp(i64::from(v), 0);
                Ok(time)
            }
            ValueRef::DateTime64(v, params) => {
                let (precision, tz) = *params;
                Ok(to_datetime(v, precision, tz))
            }
            _ => {
                let from = SqlType::from(value).to_string();
                Err(Error::FromSql(FromSqlError::InvalidType {
                    src: from,
                    dst: "DateTime<Tz>".into(),
                }))
            }
        }
    }
}

from_sql_impl! {
    u8: UInt8,
    u16: UInt16,
    u32: UInt32,
    u64: UInt64,

    i8: Int8,
    i16: Int16,
    i32: Int32,
    i64: Int64,

    f32: Float32,
    f64: Float64
}

fn type_of<T>() -> String {
    format!("{}", std::any::type_name::<T>())
}

macro_rules! replace_expr {
    ($_tparam:ident $sub:expr) => {
        $sub
    };
}

macro_rules! tuple_len {
    ($($tparam:ident),+) => {
        <[()]>::len(&[$(replace_expr!($tparam ())),+])
    }
}

macro_rules! from_sql_tuple_impl {
    ($($tparam:ident),+) => {
        impl <'a, $($tparam,)+> FromSql<'a> for ($($tparam,)+)
        where
            $(
                $tparam: FromSql<'a>,
            )+
        {
            fn from_sql(value: ValueRef<'a>) -> FromSqlResult<Self> {
                const TLEN: usize = tuple_len!($($tparam),+);
                match value {
                    ValueRef::Tuple(vs) if vs.len() == TLEN => {
                        let mut seq = 0..TLEN;
                        Ok(($(
                            <$tparam>::from_sql(vs[seq.next().unwrap()].clone())?,
                        )+))
                    },
                    _ => {
                        let from = SqlType::from(value).to_string();
                        Err(Error::FromSql(FromSqlError::InvalidType {
                            src: from,
                            dst: type_of::<($($tparam,)+)>().into(),
                        }))
                    }
                }
            }
        }
    };
}

macro_rules! from_sql_vec_of_tuples_impl {
    ($($tparam:ident),+) => {
        impl <'a, $($tparam,)+> FromSql<'a> for Vec<($($tparam,)+)>
        where
            $(
                $tparam: FromSql<'a>,
            )+
        {
            fn from_sql(value: ValueRef<'a>) -> FromSqlResult<Self> {
                match value {
                    ValueRef::Array(SqlType::Tuple(_), vs) => {
                        let mut result = Vec::with_capacity(vs.len());
                        for v in vs.iter() {
                            let value: ($($tparam,)+) = <($($tparam,)+)>::from_sql(v.clone())?;
                            result.push(value);
                        }
                        Ok(result)
                    }
                    _ => {
                        let from = SqlType::from(value.clone()).to_string();
                        Err(Error::FromSql(FromSqlError::InvalidType {
                            src: from,
                            dst: format!("Vec<{}>", type_of::<($($tparam,)+)>()).into(),
                        }))
                    }
                }
            }
        }
    };
}

from_sql_tuple_impl!(T1);
from_sql_tuple_impl!(T1, T2);
from_sql_tuple_impl!(T1, T2, T3);
from_sql_tuple_impl!(T1, T2, T3, T4);
from_sql_tuple_impl!(T1, T2, T3, T4, T5);
from_sql_tuple_impl!(T1, T2, T3, T4, T5, T6);
from_sql_tuple_impl!(T1, T2, T3, T4, T5, T6, T7);
from_sql_tuple_impl!(T1, T2, T3, T4, T5, T6, T7, T8);
from_sql_tuple_impl!(T1, T2, T3, T4, T5, T6, T7, T8, T9);
from_sql_tuple_impl!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10);
from_sql_tuple_impl!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11);
from_sql_tuple_impl!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12);

from_sql_vec_of_tuples_impl!(T1);
from_sql_vec_of_tuples_impl!(T1, T2);
from_sql_vec_of_tuples_impl!(T1, T2, T3);
from_sql_vec_of_tuples_impl!(T1, T2, T3, T4);
from_sql_vec_of_tuples_impl!(T1, T2, T3, T4, T5);
from_sql_vec_of_tuples_impl!(T1, T2, T3, T4, T5, T6);
from_sql_vec_of_tuples_impl!(T1, T2, T3, T4, T5, T6, T7);
from_sql_vec_of_tuples_impl!(T1, T2, T3, T4, T5, T6, T7, T8);
from_sql_vec_of_tuples_impl!(T1, T2, T3, T4, T5, T6, T7, T8, T9);
from_sql_vec_of_tuples_impl!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10);
from_sql_vec_of_tuples_impl!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11);
from_sql_vec_of_tuples_impl!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12);

// impl<'a, T1, T2, T3> FromSql<'a> for (T1, T2, T3)
// where
//     T1: FromSql<'a>,
//     T2: FromSql<'a>,
//     T3: FromSql<'a>,
// {
//     fn from_sql(value: ValueRef<'a>) -> FromSqlResult<Self> {
//         match value {
//             ValueRef::Tuple(vs) if vs.len() == 3 => {
//                 let item1 = T1::from_sql(vs[0].1.clone())?;
//                 let item2 = T2::from_sql(vs[1].1.clone())?;
//                 let item3 = T3::from_sql(vs[2].1.clone())?;
//                 Ok((item1, item2, item3))
//             }
//             _ => {
//                 let from = SqlType::from(value).to_string();
//                 Err(Error::FromSql(FromSqlError::InvalidType {
//                     src: from,
//                     dst: type_of::<(T1, T2, T3)>().into(),
//                 }))
//             }
//         }
//     }
// }

// impl<'a, T1, T2, T3> FromSql<'a> for Vec<(T1, T2, T3)>
// where
//     T1: FromSql<'a>,
//     T2: FromSql<'a>,
//     T3: FromSql<'a>,
// {
//     fn from_sql(value: ValueRef<'a>) -> FromSqlResult<Self> {
//         match value {
//             ValueRef::Array(SqlType::Tuple(_), vs) => {
//                 let mut result = Vec::with_capacity(vs.len());
//                 for v in vs.iter() {
//                     let value: (T1, T2, T3) = <(T1, T2, T3)>::from_sql(v.clone())?;
//                     result.push(value);
//                 }
//                 Ok(result)
//             }
//             _ => {
//                 let from = SqlType::from(value.clone()).to_string();
//                 Err(Error::FromSql(FromSqlError::InvalidType {
//                     src: from,
//                     dst: format!("Vec<{}>", type_of::<(T1, T2, T3)>()).into(),
//                 }))
//             }
//         }
//     }
// }

#[cfg(test)]
mod test {
    use crate::types::{from_sql::FromSql, ValueRef};

    #[test]
    fn test_u8() {
        let v = ValueRef::from(42_u8);
        let actual = u8::from_sql(v).unwrap();
        assert_eq!(actual, 42_u8);
    }

    #[test]
    fn test_bad_convert() {
        let v = ValueRef::from(42_u16);
        match u32::from_sql(v) {
            Ok(_) => panic!("should fail"),
            Err(e) => assert_eq!(
                "From SQL error: `SqlType::UInt16 cannot be cast to u32.`".to_string(),
                format!("{}", e)
            ),
        }
    }
}
