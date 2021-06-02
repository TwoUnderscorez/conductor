-- --------------------------------------------------------------------------------------------------------------
-- SCHEMA FOR SEMAPHORE DAO
-- --------------------------------------------------------------------------------------------------------------

CREATE TABLE semaphore (
    created_on TIMESTAMP(3) DEFAULT NOW(3),
    semaphore_name varchar(255) NOT NULL,
    holder_name varchar(255) NOT NULL,
    INDEX holder_name_idx (holder_name),
    INDEX semaphore_name_idx (semaphore_name)
);
