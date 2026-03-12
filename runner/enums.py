from enum import Enum, unique, IntFlag

@unique
class OperatingSystem(Enum):
    """
    The operating system to use.
    """
    centos_7 = 1
    rhel_8 = 2
    ubuntu_2004 = 3
    mac_native = 4
    rhel_9 = 5
    elxr_12 = 6

    def is_rhel(self):
        return self == OperatingSystem.rhel_8 or self == OperatingSystem.rhel_9

    def rhel_version(self):
        if not self.is_rhel():
            return None
        if self == OperatingSystem.rhel_9:
            return 9
        return 8

@unique
class HostDatabase(Enum):
    """
    The host database to use.
    """
    psql_15 = 1

@unique
class Architecture(Enum):
    """
    The architecture to use.
    """
    amd64 = 1
    arm64 = 2

    def get_docker_platform(self):
        return f"linux/{self.name.lower()}"

@unique
class BuildingType(Enum):
    """
    The way to build our code.

    Release          - Release build
    RelWithAssert    - Release build with assertions - your default development build type
    Debug            - Debug build - unoptimized build to ease debugging
    Coverage         - Coverage build - build to get coverage data
    Sanitizers       - Sanitizers build - similar as RelWithAssert, but includes sanitizers built in for extra safety checks.
    """
    Release = 1
    RelWithAssert = 2
    Debug = 3
    Coverage = 4
    Sanitizers = 5

class BuildCleaning(IntFlag):
    """
    What to wipe before a build.

    data           - wipe the PG data directories
    build          - wipe the build directory
    artifacts      - wipes the artifacts in the runner directory
    build_cache    - wipe the build cache (ccache)
    deterministic  - wipe everything needed for a reproducible build
                     (data, build, artifacts) but keep ccache
    nuclear        - nuclear option: wipe everything (deterministic + ccache)
    """
    nothing       = 0
    data          = 1
    build         = 2
    artifacts     = 4
    build_cache   = 8          # ccache
    tests         = 16

    # grouped presets ──────────────────────────────────────────────────────────
    deterministic = data | build | artifacts | tests  # the 99.999 % case
    nuclear       = deterministic | build_cache
