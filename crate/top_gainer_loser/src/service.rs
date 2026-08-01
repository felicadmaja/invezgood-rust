pub struct TopGainerLoserService;

impl TopGainerLoserService {
    pub fn new() -> Self {
        Self
    }
}

#[tonic::async_trait]
impl crate::pb::top_gainer_loser_server::TopGainerLoser for TopGainerLoserService {}
