-- schema.sql

-- 1. Create the Posts table
create table if not exists POSTS (
    ID             TEXT primary key,
    SUBREDDIT      TEXT not null,
    TITLE          TEXT,
    FLAIR          TEXT,
    DESCRIPTION    TEXT,
    URL            TEXT,
    UPVOTES        integer,
    COMMENTS_COUNT integer,
    AUTHOR         TEXT,
    TIMESTAMP      TEXT,
    CRAWLED_AT     TEXT
);

-- 2. Create the Comments table
create table if not exists COMMENTS (
    ID         TEXT primary key,
    POST_ID    TEXT not null,
    PARENT_ID  TEXT,
    AUTHOR     TEXT,
    SCORE      integer,
    TEXT       TEXT,
    DEPTH      integer,
    CRAWLED_AT TEXT,
    foreign key ( POST_ID )
        references POSTS ( ID ),
    foreign key ( PARENT_ID )
        references COMMENTS ( ID )
);

-- 3. Create indexes to make querying fast for your NLP models
create index if not exists IDX_COMMENTS_POST on
    COMMENTS (
        POST_ID
    );
create index if not exists IDX_COMMENTS_PARENT on
    COMMENTS (
        PARENT_ID
    );

-- Store output from sentiment and consensus calculations --
create table if not exists POST_METRICS (
    POST_ID                    TEXT primary key,
    SUBREDDIT                  TEXT not null,
    VADER_SENTIMENT            real,
    FINBERT_NET                real,
    COMMENT_WEIGHTED_SENTIMENT real,
    CONSENSUS_DIVERGENCE       real,
    SCORED_AT                  TEXT,
    foreign key ( POST_ID )
        references POSTS ( ID )
);

-- Stores the individual decomposed assertions and their evidence --
create table if not exists CLAIM_VERIFICATIONS (
    CLAIM_ID         integer primary key AUTOINCREMENT,
    POST_ID          TEXT not null,
    EXTRACTED_CLAIM  TEXT not null,
    VERACITY         TEXT not null,         -- SUPPORTED, REFUTED, NOT ENOUGH INFO
    CONFIDENCE       real,
    EVIDENCE_SOURCE  TEXT,
    EVIDENCE_SNIPPET TEXT,
    VERIFIED_AT      TEXT,
    foreign key ( POST_ID )
        references POSTS ( ID )
);

-- Stores aggregated figures so the frontend does not have to compute group-by statistics --
create table if not exists SUBREDDIT_DAILY_TRENDS (
    DATE                   TEXT not null,
    SUBREDDIT              TEXT not null,
    AVG_POST_SENTIMENT     real,
    AVG_COMMENT_SENTIMENT  real,
    TOTAL_CLAIMS_VERIFIED  integer,
    SUPPORTED_CLAIMS_COUNT integer,
    REFUTED_CLAIMS_COUNT   integer,
    TOP_MENTIONED_TOKEN    TEXT,
    primary key ( DATE,
                  SUBREDDIT )
);