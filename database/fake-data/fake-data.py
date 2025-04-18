import os
import random
import psycopg2
from dotenv import load_dotenv
from datetime import datetime, timedelta

# Configuration variables
STREAMS_COUNT = 100
PACKETS_COUNT = 1000
TAGS_COUNT = 50
SERVICES_COUNT = 20
TAGS_PER_PACKET = 3  # Average number of tags per packet

# Load environment variables
load_dotenv()

# Database connection parameters
DB_PARAMS = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT')
}

def generate_ip():
    """Generate random IP address"""
    return f"{random.randint(1, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}"

def generate_port():
    """Generate random port number"""
    return random.randint(1024, 65535)

def generate_protocol():
    """Generate random protocol"""
    return random.choice(['TCP', 'UDP'])

def generate_packet_data():
    """Generate random packet data"""
    return bytes([random.randint(0, 255) for _ in range(random.randint(20, 1500))])

def generate_tag_name():
    """Generate random tag name"""
    prefixes = ['Security', 'Network', 'System', 'Application', 'Performance']
    suffixes = ['Alert', 'Warning', 'Error', 'Info', 'Debug']
    return f"{random.choice(prefixes)} {random.choice(suffixes)}"

def generate_template():
    """Generate random template"""
    templates = [
        "IP: {ip}, Port: {port}",
        "Protocol: {protocol}, Count: {count}",
        "Source: {src}, Destination: {dst}",
        "Time: {time}, Status: {status}"
    ]
    return random.choice(templates)

def generate_service_name():
    """Generate random service name"""
    services = ['HTTP', 'HTTPS', 'SSH', 'FTP', 'SMTP', 'DNS', 'NTP', 'LDAP', 'RDP', 'VNC']
    return random.choice(services)

def insert_fake_services(cursor):
    """Insert fake services into database"""
    service_ids = []
    used_ports = set()
    
    for i in range(SERVICES_COUNT):
        while True:
            port = generate_port()
            if port not in used_ports:
                used_ports.add(port)
                break
        
        cursor.execute(
            "INSERT INTO service (name, port, chunked_transfer_encoding, urldecode, "
            "merge_adjacent_packets, inflate_websockets, decrypt_tls) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id",
            (
                generate_service_name(),
                port,
                random.choice([True, False]),
                random.choice([True, False]),
                random.choice([True, False]),
                random.choice([True, False]),
                random.choice([True, False])
            )
        )
        service_ids.append(cursor.fetchone()[0])
    return service_ids

def insert_fake_tag_names(cursor):
    """Insert fake tag names into database"""
    tag_name_ids = []
    for _ in range(TAGS_COUNT):
        cursor.execute(
            "INSERT INTO tag_name (name, color, template_type, template) "
            "VALUES (%s, %s, %s, %s) RETURNING id",
            (
                generate_tag_name(),
                random.randint(0, 360),
                random.choice(['text', 'json']),
                generate_template()
            )
        )
        tag_name_ids.append(cursor.fetchone()[0])
    return tag_name_ids

def insert_fake_streams(cursor, service_ids):
    """Insert fake streams into database"""
    stream_ids = []
    for _ in range(STREAMS_COUNT):
        cursor.execute(
            "INSERT INTO stream (src_ip, src_port, dst_ip, dst_port, protocol, active, packet_count, service_id) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING id",
            (
                generate_ip(),
                generate_port(),
                generate_ip(),
                generate_port(),
                generate_protocol(),
                random.choice([True, False]),
                random.randint(0, 1000),
                random.choice(service_ids) if service_ids else None
            )
        )
        stream_ids.append(cursor.fetchone()[0])
    return stream_ids

def insert_fake_packets(cursor, stream_ids):
    """Insert fake packets into database"""
    packet_ids = []
    for _ in range(PACKETS_COUNT):
        timestamp = int((datetime.now() - timedelta(days=random.randint(0, 30))).timestamp() * 1000)
        cursor.execute(
            "INSERT INTO packets (timestamp, direction, stream_id, packet_data) "
            "VALUES (%s, %s, %s, %s) RETURNING id",
            (
                timestamp,
                random.choice(['from', 'dest']),
                random.choice(stream_ids),
                generate_packet_data()
            )
        )
        packet_ids.append(cursor.fetchone()[0])
    return packet_ids

def insert_fake_tags(cursor, packet_ids, tag_name_ids):
    """Insert fake tags into database"""
    for packet_id in packet_ids:
        # Generate random number of tags for each packet
        num_tags = random.randint(0, TAGS_PER_PACKET * 2)  # Double the average to get some variation
        for _ in range(num_tags):
            packet_data_length = random.randint(20, 1500)
            start_index = random.randint(0, packet_data_length - 1)
            end_index = random.randint(start_index + 1, packet_data_length)
            
            cursor.execute(
                "INSERT INTO tag (packet_id, tag_id, start_index, end_index) "
                "VALUES (%s, %s, %s, %s)",
                (
                    packet_id,
                    random.choice(tag_name_ids),
                    start_index,
                    end_index
                )
            )

def main():
    try:
        # Connect to database
        conn = psycopg2.connect(**DB_PARAMS)
        cursor = conn.cursor()

        # Insert fake data
        service_ids = insert_fake_services(cursor)
        tag_name_ids = insert_fake_tag_names(cursor)
        stream_ids = insert_fake_streams(cursor, service_ids)
        packet_ids = insert_fake_packets(cursor, stream_ids)
        insert_fake_tags(cursor, packet_ids, tag_name_ids)

        # Commit changes
        conn.commit()
        print("Fake data has been successfully inserted!")

    except Exception as e:
        print(f"Error occurred: {e}")
        if 'conn' in locals():
            conn.rollback()
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()
