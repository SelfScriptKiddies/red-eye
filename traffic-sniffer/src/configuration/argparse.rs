use clap::{Command, Arg, ArgAction};

pub(crate) fn initialize_cli_args() -> Command {
    Command::new("red-eye-client")
        .version("0.0.1")
        .author("TonySD")
        .about("Client for traffic analyzer red-eye")
        .arg(
            Arg::new("interface")
                .short('i')
                .long("interface")
                .value_name("interface")
                .action(ArgAction::Set)
                .help("Choose the interface, that will be sniffed"),
        )
        .arg(
            Arg::new("machine_ip")
                .short('I')
                .long("ip")
                .value_name("machine_ip")
                .action(ArgAction::Set)
                .help("Machine ip on chose interface"),
        )
        .arg(
            Arg::new("kafka_broker_host")
                .short('b')
                .long("broker-host")
                .value_name("kafka_broker_host")
                .action(ArgAction::Set)
                .help("Kafka broker host with ip. Example: localhost:29092"),
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
