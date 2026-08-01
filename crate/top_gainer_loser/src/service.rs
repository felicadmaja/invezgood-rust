use std::sync::Arc;

use scylla::client::session::Session;
use tonic::{Request, Response, Status};

use crate::model::TopGainerLoserRow as DbTopGainerLoserRow;
use crate::pb::top_gainer_loser_server::TopGainerLoser;
use crate::pb::{
    GetTopGainerLoserRequest, GetTopGainerLoserResponse, GraphPoint, TopGainerLoserRow,
};

pub struct TopGainerLoserService {
    session: Arc<Session>,
}

impl TopGainerLoserService {
    pub fn new(session: Arc<Session>) -> Self {
        Self { session }
    }

    fn db_row_to_proto(row: DbTopGainerLoserRow) -> TopGainerLoserRow {
        TopGainerLoserRow {
            tahun_bulan_tanggal: row.tahun_bulan_tanggal.format("%Y-%m-%d").to_string(),
            code: row.code,
            name: row.name.unwrap_or_default(),
            price: row.price.unwrap_or_default(),
            change: row.change_pct.unwrap_or_default(),
            value: row.value.unwrap_or_default(),
            volume: row.volume.unwrap_or_default(),
            logo: row.logo.unwrap_or_default(),
            calculated_value: row.calculated_value.unwrap_or_default(),
            tipe: row.tipe.unwrap_or_default(),
            graph: row
                .graph
                .unwrap_or_default()
                .into_iter()
                .map(|(date, value)| GraphPoint { date, value })
                .collect(),
        }
    }
}

#[tonic::async_trait]
impl TopGainerLoser for TopGainerLoserService {
    async fn get_top_gainer_loser(
        &self,
        request: Request<GetTopGainerLoserRequest>,
    ) -> Result<Response<GetTopGainerLoserResponse>, Status> {
        let tahun_bulan_tanggal = request.into_inner().tahun_bulan_tanggal;

        let rows = crate::invezgo::fetch_and_save(self.session.clone(), tahun_bulan_tanggal)
            .await
            .map_err(Status::internal)?;

        let items = rows.into_iter().map(Self::db_row_to_proto).collect();

        Ok(Response::new(GetTopGainerLoserResponse { items }))
    }
}
