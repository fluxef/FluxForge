use crate::core::{ForgeConfig, ForgeSchema, ForgeUniversalValue};
use crate::DatabaseDriver;
use async_trait::async_trait;
use futures::Stream;
use indexmap::IndexMap;
use sqlx::PgPool;
use std::error::Error;
use std::pin::Pin;

pub struct PostgresDriver {
    pub pool: PgPool,
}

impl PostgresDriver {
    // local functions not part of the trait
}

#[async_trait]
impl DatabaseDriver for PostgresDriver {
    async fn db_is_empty(&self) -> Result<bool, Box<dyn Error>> {
        todo!()
    }

    async fn fetch_schema(&self, config: &ForgeConfig) -> Result<ForgeSchema, Box<dyn Error>> {
        todo!()
    }

    async fn create_schema(
        &self,
        source_schema: &ForgeSchema,
        config: &ForgeConfig,
        dry_run: bool,
    ) -> Result<Vec<String>, Box<dyn Error>> {
        todo!()
    }

    async fn diff_schema(
        &self,
        schema: &ForgeSchema,
        config: &ForgeConfig,
        dry_run: bool,
        destructive: bool,
    ) -> Result<Vec<String>, Box<dyn Error>> {
        todo!()
    }

    async fn stream_table_data(
        &self,
        table_name: &str,
    ) -> Result<
        Pin<
            Box<
                dyn Stream<Item = Result<IndexMap<String, ForgeUniversalValue>, sqlx::Error>>
                    + Send
                    + '_,
            >,
        >,
        Box<dyn Error>,
    > {
        todo!()
    }

    async fn insert_chunk(
        &self,
        table_name: &str,
        dry_run: bool,
        halt_on_error: bool,
        chunk: Vec<IndexMap<String, ForgeUniversalValue>>,
    ) -> Result<(), Box<dyn Error>> {
        todo!()
    }
}
