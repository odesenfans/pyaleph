import argparse
import os
import sys
import yaml
from typing import Callable, Dict, TypeVar


def cli_parse() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extracts the image tag for the specified service out of a docker-compose file."
    )
    parser.add_argument(
        "service",
        action="store",
        type=str,
        help="Service as defined in the docker-compose file.",
    )
    parser.add_argument(
        "--file",
        "-f",
        action="store",
        default="docker-compose.yml",
        type=str,
        help="Path to the Docker Compose file.",
    )
    return parser.parse_args()


T = TypeVar("T")


def get_key(
    dictionary: Dict[str, T],
    key: str,
    error_message_generator: Callable[[KeyError], str],
) -> T:
    try:
        return dictionary[key]
    except KeyError as e:
        error_message = error_message_generator(e)
        print(error_message, file=sys.stderr)
        sys.exit(1)


def main(args: argparse.Namespace):
    service_name = args.service
    compose_file = args.file

    if not os.path.isfile(compose_file):
        print(
            f"The specified docker-compose file could not be found: '{compose_file}'.",
            file=sys.stderr,
        )
        sys.exit(1)

    with open(compose_file) as f:
        compose_values = yaml.safe_load(f)

    services = get_key(
        compose_values,
        "services",
        lambda e: "The docker-compose file does not have a 'services' field.",
    )
    service = get_key(
        services, service_name, lambda e: f"No such service: '{service_name}'."
    )
    image = get_key(
        service, "image", lambda e: f"No image specified for service {service_name}."
    )

    print(image)


if __name__ == "__main__":
    main(cli_parse())
