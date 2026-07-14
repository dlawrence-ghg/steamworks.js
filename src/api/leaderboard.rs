use napi_derive::napi;

#[napi]
pub mod leaderboard {
    use napi::bindgen_prelude::{BigInt, Error};
    use std::collections::HashMap;
    use std::sync::{Mutex, OnceLock};
    use tokio::sync::oneshot;

    #[napi]
    pub enum LeaderboardSortMethod {
        Ascending,
        Descending,
    }

    #[napi]
    pub enum LeaderboardDisplayType {
        Numeric,
        TimeSeconds,
        TimeMilliSeconds,
    }

    #[napi]
    pub enum LeaderboardUploadMethod {
        KeepBest,
        ForceUpdate,
    }

    #[napi]
    pub enum LeaderboardDataRequest {
        Global,
        GlobalAroundUser,
        Friends,
    }

    #[napi(object)]
    pub struct LeaderboardUploadResult {
        pub score: i32,
        pub was_changed: bool,
        pub global_rank_new: i32,
        pub global_rank_previous: i32,
    }

    #[napi(object)]
    pub struct LeaderboardEntry {
        pub steam_id: BigInt,
        pub global_rank: i32,
        pub score: i32,
        pub details: Vec<i32>,
    }

    // steamworks::user_stats::Leaderboard's inner handle is crate-private, so JS
    // can't round-trip handles. Cache them by board name instead — every function
    // takes the name (+ sort/display so a missing board is created consistently).
    fn cache() -> &'static Mutex<HashMap<String, steamworks::Leaderboard>> {
        static CACHE: OnceLock<Mutex<HashMap<String, steamworks::Leaderboard>>> = OnceLock::new();
        CACHE.get_or_init(|| Mutex::new(HashMap::new()))
    }

    fn to_sort(sort: LeaderboardSortMethod) -> steamworks::LeaderboardSortMethod {
        match sort {
            LeaderboardSortMethod::Ascending => steamworks::LeaderboardSortMethod::Ascending,
            LeaderboardSortMethod::Descending => steamworks::LeaderboardSortMethod::Descending,
        }
    }

    fn to_display(display: LeaderboardDisplayType) -> steamworks::LeaderboardDisplayType {
        match display {
            LeaderboardDisplayType::Numeric => steamworks::LeaderboardDisplayType::Numeric,
            LeaderboardDisplayType::TimeSeconds => steamworks::LeaderboardDisplayType::TimeSeconds,
            LeaderboardDisplayType::TimeMilliSeconds => {
                steamworks::LeaderboardDisplayType::TimeMilliSeconds
            }
        }
    }

    async fn resolve(
        name: String,
        sort: LeaderboardSortMethod,
        display: LeaderboardDisplayType,
    ) -> Result<steamworks::Leaderboard, Error> {
        if let Some(found) = cache().lock().unwrap().get(&name) {
            return Ok(found.clone());
        }

        let client = crate::client::get_client();
        let (tx, rx) = oneshot::channel();
        client
            .user_stats()
            .find_or_create_leaderboard(&name, to_sort(sort), to_display(display), |result| {
                tx.send(result).ok();
            });

        match rx.await.unwrap() {
            Ok(Some(handle)) => {
                cache().lock().unwrap().insert(name, handle.clone());
                Ok(handle)
            }
            Ok(None) => Err(Error::from_reason(format!(
                "leaderboard \"{name}\" not found and could not be created"
            ))),
            Err(e) => Err(Error::from_reason(e.to_string())),
        }
    }

    /// Find the leaderboard, creating it with the given sort/display if it
    /// doesn't exist yet. Returns the raw 64-bit handle. Handles are cached by
    /// name for the lifetime of the process.
    #[napi]
    pub async fn find_or_create(
        name: String,
        sort: LeaderboardSortMethod,
        display: LeaderboardDisplayType,
    ) -> Result<BigInt, Error> {
        let handle = resolve(name, sort, display).await?;
        Ok(BigInt::from(handle.raw()))
    }

    /// Upload a score (finding/creating the board first if needed). Returns the
    /// upload result, or an error if Steam rejected the score.
    #[napi]
    pub async fn upload_score(
        name: String,
        sort: LeaderboardSortMethod,
        display: LeaderboardDisplayType,
        method: LeaderboardUploadMethod,
        score: i32,
    ) -> Result<LeaderboardUploadResult, Error> {
        let handle = resolve(name, sort, display).await?;
        let method = match method {
            LeaderboardUploadMethod::KeepBest => steamworks::UploadScoreMethod::KeepBest,
            LeaderboardUploadMethod::ForceUpdate => steamworks::UploadScoreMethod::ForceUpdate,
        };

        let client = crate::client::get_client();
        let (tx, rx) = oneshot::channel();
        client
            .user_stats()
            .upload_leaderboard_score(&handle, method, score, &[], |result| {
                tx.send(result).ok();
            });

        match rx.await.unwrap() {
            Ok(Some(uploaded)) => Ok(LeaderboardUploadResult {
                score: uploaded.score,
                was_changed: uploaded.was_changed,
                global_rank_new: uploaded.global_rank_new,
                global_rank_previous: uploaded.global_rank_previous,
            }),
            Ok(None) => Err(Error::from_reason("score upload rejected".to_string())),
            Err(e) => Err(Error::from_reason(e.to_string())),
        }
    }

    /// Download entries [start, end] (1-based ranks for Global requests).
    #[napi]
    pub async fn download_entries(
        name: String,
        sort: LeaderboardSortMethod,
        display: LeaderboardDisplayType,
        request: LeaderboardDataRequest,
        start: u32,
        end: u32,
    ) -> Result<Vec<LeaderboardEntry>, Error> {
        let handle = resolve(name, sort, display).await?;
        let request = match request {
            LeaderboardDataRequest::Global => steamworks::LeaderboardDataRequest::Global,
            LeaderboardDataRequest::GlobalAroundUser => {
                steamworks::LeaderboardDataRequest::GlobalAroundUser
            }
            LeaderboardDataRequest::Friends => steamworks::LeaderboardDataRequest::Friends,
        };

        let client = crate::client::get_client();
        let (tx, rx) = oneshot::channel();
        client.user_stats().download_leaderboard_entries(
            &handle,
            request,
            start as usize,
            end as usize,
            64,
            |result| {
                tx.send(result).ok();
            },
        );

        match rx.await.unwrap() {
            Ok(entries) => Ok(entries
                .into_iter()
                .map(|e| LeaderboardEntry {
                    steam_id: BigInt::from(e.user.raw()),
                    global_rank: e.global_rank,
                    score: e.score,
                    details: e.details,
                })
                .collect()),
            Err(e) => Err(Error::from_reason(e.to_string())),
        }
    }
}
