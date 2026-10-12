$version: "2"
$operationInputSuffix: "Request"
$operationOutputSuffix: "Response"

namespace ducklake.rest

use ducklake.rest.codegen#cppClass
use ducklake.rest.codegen#cppMember
use ducklake.rest.codegen#cppType

/// Expires snapshots.
///
/// Removes the snapshots, together with the catalog entries and files that are only visible in them. The data files are
/// scheduled for deletion.
@http(method: "POST", uri: "/v1/expire_snapshots")
operation ExpireSnapshots {
    input := {
        /// The snapshots to expire.
        @required
        snapshot_ids: UInt64List
    }

    output := {
        /// The number of expired snapshots.
        @required
        expired_snapshots: UInt64
    }
}

/// Returns the files scheduled for deletion.
@http(method: "POST", uri: "/v1/files_for_cleanup")
operation FilesForCleanup {
    input := {
        /// Only files scheduled for deletion before this timestamp.
        @cppType("string")
        older_than: Timestamp
    }

    output := {
        /// The files.
        files: FileForCleanupList = []
    }
}

/// Removes files from the files scheduled for deletion, after the client deleted them.
@http(method: "POST", uri: "/v1/remove_cleaned_up_files")
operation RemoveCleanedUpFiles {
    input := {
        /// The files.
        @required
        data_file_ids: UInt64List
    }
}

/// Returns the paths of all files that the catalog refers to.
///
/// These are the data files, delete files and files scheduled for deletion, so a client can find orphaned files in the
/// data path.
@http(method: "POST", uri: "/v1/known_files")
operation KnownFiles {
    output := {
        /// The paths.
        paths: StringList = []
    }
}

/// A file that is scheduled for deletion.
@cppClass("DuckLakeFileForCleanup")
structure FileForCleanup {
    /// The id of the file.
    @required
    @cppMember("id")
    data_file_id: UInt64

    /// The path of the file.
    @required
    path: String

    /// When the file was scheduled for deletion.
    @required
    @cppMember("time")
    schedule_start: Timestamp
}
