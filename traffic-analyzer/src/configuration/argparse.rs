use clap::{Command, Arg, ArgAction};

pub(crate) fn initialize_cli_args() -> Command {
    Command::new("red-eye-analyzer")
        .version("0.0.1")
        .author("TonySD")
        .about("Red-eye traffic analyzer red-eye")
        .arg(
            Arg::new("db_host")
                .short('H')
                .long("db-host")
                .action(ArgAction::Set)
                .help("Database ip or domain"),
        )
        .arg(
            Arg::new("db_port")
                .short('P')
                .long("db-port")
                .action(ArgAction::Set)
                .help("Database port"),
        )
        .arg(
            Arg::new("db_username")
                .short('u')
                .long("db-username")
                .action(ArgAction::Set)
                .help("Database username"),
        )
        .arg(
            Arg::new("db_password")
                .short('p')
                .long("db-password")
                .action(ArgAction::Set)
                .help("Database password"),
        )
        .arg(
            Arg::new("db_schema")
                .short('s')
                .long("db-schema")
                .action(ArgAction::Set)
                .help("Database schema"),
        )
        .arg(
            Arg::new("kafka_host")
                .short('b')
                .long("broker-host")
                .action(ArgAction::Set)
                .help("Kafka broker host"),
        )
        .arg(
            Arg::new("kafka_topic")
                .short('t')
                .long("broker-topic")
                .value_name("kafka_topic")
                .action(ArgAction::Set)
                .help("Topic name, where will be sent raw packets"),
        )
}