use clap::{Command, Arg, ArgAction};

pub(crate) fn initialize_cli_args() -> Command {
    Command::new("red-eye-client")
        .version("0.0.1")
        .author("TonySD")
        .about("Client for traffic analyzer red-eye")
        .arg(
            Arg::new("db_host")
                .short('H')
                .long("host")
                .action(ArgAction::Set)
                .value_name("db_host")
                .help("Database hostname or ip"),
        )
        .arg(
            Arg::new("db_port")
                .short('P')
                .long("port")
                .value_name("db_port")
                .action(ArgAction::Set)
                .help("Database port"),
        )
        .arg(
            Arg::new("db_username")
                .short('u')
                .long("username")
                .value_name("db_username")
                .action(ArgAction::Set)
                .help("Database username"),
        )
        .arg(
            Arg::new("db_password")
                .short('p')
                .long("password")
                .value_name("db_password")
                .action(ArgAction::Set)
                .help("Database password"),
        )
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
}
