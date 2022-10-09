from enum import Enum


class FileType(str, Enum):
    FILE = "file"
    DIRECTORY = "dir"
