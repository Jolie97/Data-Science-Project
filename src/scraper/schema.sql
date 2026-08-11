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