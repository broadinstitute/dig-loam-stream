package loamstream.googlecloud

/**
 * @author clint
 * Nov 28, 2016
 */
final case class GoogleCloudConfig(
    projectId: String, //broadinstitute.com:cmi-gce-01
    clusterId: String, //"minimal"
    numWorkers: Int, //2
    zone: String = "us-central1-f",
    masterMachineType: String = "n1-standard-1",
    masterBootDiskSize: Int = 20, //gigs?
    workerMachineType: String ="n1-standard-1",
    workerBootDiskSize: Int = 20, //gigs?
    imageVersion: String = "1.0",
    scopes: String = "https://www.googleapis.com/auth/cloud-platform")