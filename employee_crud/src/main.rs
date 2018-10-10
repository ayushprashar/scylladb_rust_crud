#[macro_use]
extern crate cdrs;
#[macro_use]
extern crate cdrs_helpers_derive;

use cdrs::authenticators::NoneAuthenticator;
use cdrs::cluster::session::{new as new_session, Session};
use cdrs::cluster::{ClusterTcpConfig, NodeTcpConfigBuilder, TcpConnectionPool};
use cdrs::load_balancing::RoundRobin;
use cdrs::query::*;

use cdrs::frame::IntoBytes;
use cdrs::types::from_cdrs::FromCDRSByName;
use cdrs::types::prelude::*;

type CurrentSession = Session<RoundRobin<TcpConnectionPool<NoneAuthenticator>>>;

#[derive(Debug, Clone, PartialEq, IntoCDRSValue, TryFromUDT)]
struct User {
    name: String,
    age: i32
}

#[derive(Clone, Debug, IntoCDRSValue, TryFromRow, PartialEq)]
struct RowStruct {
    id: i32,
    user: User
}

fn get_session() -> CurrentSession {
    let node = NodeTcpConfigBuilder::new("127.0.0.1:9042",
                                         NoneAuthenticator {}).build();
    let cluster_config = ClusterTcpConfig(vec![node]);
    new_session(&cluster_config, RoundRobin::new())
        .expect("session should be created")
}

fn create_keyspace(session: &CurrentSession) {
    let create_ks: &'static str = "CREATE KEYSPACE IF NOT EXISTS EMPLOYEE WITH REPLICATION = {\
    'class' : 'SimpleStrategy', 'replication_factor' : 1};";
    session.query(create_ks).expect("Keyspace creation error");
}

fn create_udt(session: &CurrentSession) {
    let create_type_cql = "CREATE TYPE IF NOT EXISTS EMPLOYEE.emp (name text, age int)";
    session
        .query(create_type_cql)
        .expect("Keyspace creation error");
}

fn create_table(session: &CurrentSession) {
    let create_table_cql = "CREATE TABLE IF NOT EXISTS EMPLOYEE.knoldus (ID INT PRIMARY KEY, \
    USER frozen<EMPLOYEE.emp>);";
    session
        .query(create_table_cql)
        .expect("table creation error");
}

fn insert_struct(session: &CurrentSession) {
    let row = RowStruct {
        id: 3i32,
        user: User {
            name: "Ayush".to_string(),
            age: 25i32
        }
    };

    let insert_into_db = "INSERT INTO EMPLOYEE.knoldus (ID, USER) VALUES (?,?)";
    session
        .query_with_values(insert_into_db,query_values!(row.id,row.user))
        .expect("Insert Query");
}

fn select_struct (session: &CurrentSession) {
    let select_cql = "SELECT * FROM EMPLOYEE.KNOLDUS";
    let rows = session
        .query(select_cql)
        .expect("Select query")
        .get_body()
        .expect("get body")
        .into_rows()
        .expect("into rows");
    for row in rows {
        let current_row: RowStruct = RowStruct::try_from_row(row).expect("into Rowstruct");
        print!("{:?}",current_row);
    }
}

fn update_struct(session: &CurrentSession) {
    let update_cql = "UPDATE EMPLOYEE.KNOLDUS SET USER = ? WHERE ID = ?";
    let updated_user = User {
        name: "Miral".to_string(),
        age: 24i32
    };
    let userid= 3i32;
    session
        .query_with_values(update_cql,query_values!(updated_user, userid))
        .expect("update operation");
}

fn delete_struct(session: &CurrentSession) {
    let delete_cql = "DELETE FROM EMPLOYEE.KNOLDUS WHERE ID =?";
    let id = 3i32;
    session
        .query_with_values(delete_cql,query_values!(id))
        .expect("delete");
}
fn main() {
    let session: CurrentSession = get_session();
    create_keyspace(&session);
    create_udt(&session);
    create_table(&session);
    insert_struct(&session);
    update_struct(&session);
    delete_struct(&session);
    select_struct(&session);
}
