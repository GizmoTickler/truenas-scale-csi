const { sleep, stringify } = require("../../../utils/general");
const { Zetabyte } = require("../../../utils/zfs");

// Registry namespace for cached objects
const __REGISTRY_NS__ = "TrueNASWebSocketApi";

/**
 * TrueNAS SCALE 25.04+ JSON-RPC API Wrapper
 *
 * This class provides a clean interface to TrueNAS SCALE 25.04+ API
 * using WebSocket JSON-RPC 2.0 protocol only.
 *
 * All legacy support for TrueNAS and old TrueNAS versions has been removed.
 *
 * API Documentation: https://api.truenas.com/v25.04.2/
 */
class Api {
  constructor(client, cache, options = {}) {
    this.client = client;
    this.cache = cache;
    this.options = options;
    this.ctx = options.ctx;
  }

  async getHttpClient() {
    return this.client;
  }

  /**
   * Get ZFS helper utility (for local operations only)
   */
  async getZetabyte() {
    return this.ctx.registry.get(`${__REGISTRY_NS__}:zb`, () => {
      return new Zetabyte({
        executor: {
          spawn: function () {
            throw new Error(
              "Cannot use ZFS executor directly - must use WebSocket API"
            );
          },
        },
      });
    });
  }

  /**
   * Call a JSON-RPC method on the TrueNAS API
   */
  async call(method, params = []) {
    const client = await this.getHttpClient();
    return await client.call(method, params);
  }

  /**
   * Query resources with optional filters
   * @param {string} method - The query method (e.g., "pool.dataset.query")
   * @param {array} filters - Query filters
   * @param {object} options - Query options (limit, offset, etc.)
   */
  async query(method, filters = [], options = {}) {
    return await this.call(method, [filters, options]);
  }

  /**
   * Find a single resource by properties
   * @param {string} method - The query method
   * @param {object|function} match - Properties to match or matcher function
   */
  async findResourceByProperties(method, match) {
    if (!match) {
      return null;
    }

    if (typeof match === "object" && Object.keys(match).length < 1) {
      return null;
    }

    const results = await this.query(method);

    if (!Array.isArray(results)) {
      return null;
    }

    return results.find((item) => {
      if (typeof match === "function") {
        return match(item);
      }

      for (let property in match) {
        if (match[property] !== item[property]) {
          return false;
        }
      }
      return true;
    });
  }

  // ============================================================================
  // SYSTEM VERSION & INFO
  // ============================================================================

  /**
   * Get TrueNAS system version info
   * This is cached to avoid repeated calls
   */
  async getSystemVersion() {
    const cacheKey = "truenas:system_version";
    let cached = this.cache.get(cacheKey);

    if (cached) {
      return cached;
    }

    const version = await this.call("system.version");
    this.cache.set(cacheKey, version, 300); // Cache for 5 minutes
    return version;
  }

  /**
   * Get system info (hostname, version, etc.)
   */
  async getSystemInfo() {
    return await this.call("system.info");
  }

  // ============================================================================
  // POOL & DATASET OPERATIONS
  // ============================================================================

  /**
   * Create a new dataset
   * @param {string} datasetName - Full dataset name (e.g., "pool/dataset")
   * @param {object} data - Dataset properties
   */
  async DatasetCreate(datasetName, data = {}) {
    try {
      const params = {
        name: datasetName,
        ...data,
      };

      await this.call("pool.dataset.create", [params]);
    } catch (error) {
      // Ignore "already exists" errors
      if (error.message && error.message.includes("already exists")) {
        return;
      }
      throw error;
    }
  }

  /**
   * Delete a dataset
   * @param {string} datasetName - Full dataset name
   * @param {object} data - Delete options (recursive, force, etc.)
   */
  async DatasetDelete(datasetName, data = {}) {
    try {
      await this.call("pool.dataset.delete", [datasetName, data]);
    } catch (error) {
      // Ignore "does not exist" errors
      if (error.message && error.message.includes("does not exist")) {
        return;
      }
      throw error;
    }
  }

  /**
   * Update dataset properties
   * @param {string} datasetName - Full dataset name
   * @param {object} properties - Properties to update
   */
  async DatasetSet(datasetName, properties) {
    const params = {
      ...this.getSystemProperties(properties),
      user_properties_update: this.getPropertiesKeyValueArray(
        this.getUserProperties(properties)
      ),
    };

    await this.call("pool.dataset.update", [datasetName, params]);
  }

  /**
   * Inherit a dataset property from parent
   * @param {string} datasetName - Full dataset name
   * @param {string} property - Property name to inherit
   */
  async DatasetInherit(datasetName, property) {
    const isUserProperty = this.getIsUserProperty(property);
    let params = {};

    if (isUserProperty) {
      params.user_properties_update = [{ key: property, remove: true }];
    } else {
      params[property] = { source: "INHERIT" };
    }

    await this.call("pool.dataset.update", [datasetName, params]);
  }

  /**
   * Get dataset properties
   * @param {string} datasetName - Full dataset name
   * @param {array} properties - Specific properties to retrieve (optional)
   */
  async DatasetGet(datasetName, properties = []) {
    const filters = [["id", "=", datasetName]];
    const options = {};

    if (properties && properties.length > 0) {
      options.select = properties;
    }

    const results = await this.query("pool.dataset.query", filters, options);

    if (!results || results.length === 0) {
      throw new Error(`Dataset not found: ${datasetName}`);
    }

    return results[0];
  }

  /**
   * Get dataset with children (replaces REST API /pool/dataset/id/{id} endpoint)
   * @param {string} datasetName - Full dataset name
   * @param {object} extraOptions - Additional options for query
   */
  async DatasetGetWithChildren(datasetName, extraOptions = {}) {
    const filters = [["id", "=", datasetName]];
    const options = {
      extra: {
        retrieve_children: true,
        flat: false,
        properties: extraOptions.properties || [],
        ...extraOptions,
      },
    };

    const results = await this.query("pool.dataset.query", filters, options);

    if (!results || results.length === 0) {
      return null;
    }

    return results[0];
  }

  /**
   * Get dataset with snapshots
   * @param {string} datasetName - Full dataset name
   * @param {array} snapshotProperties - Properties to fetch for snapshots
   */
  async DatasetGetWithSnapshots(datasetName, snapshotProperties = []) {
    const filters = [["id", "=", datasetName]];
    const options = {
      extra: {
        snapshots: true,
        snapshots_properties: snapshotProperties,
      },
    };

    const results = await this.query("pool.dataset.query", filters, options);

    if (!results || results.length === 0) {
      return null;
    }

    return results[0];
  }

  /**
   * List all child datasets of a parent dataset
   * @param {string} parentDatasetName - Parent dataset name
   * @param {array} properties - Properties to retrieve for each child
   */
  async DatasetListChildren(parentDatasetName, properties = []) {
    const filters = [["id", "^", parentDatasetName + "/"]];
    const options = {
      extra: {
        flat: true,
        properties: properties,
      },
    };

    return await this.query("pool.dataset.query", filters, options);
  }

  /**
   * List snapshots with optional filters
   * @param {array} filters - Query filters
   * @param {array} properties - Properties to retrieve
   */
  async SnapshotList(filters = [], properties = []) {
    const options = {};
    if (properties && properties.length > 0) {
      options.extra = {
        properties: properties,
      };
    }

    return await this.query("zfs.snapshot.query", filters, options);
  }

  /**
   * List snapshots for a dataset
   * @param {string} datasetName - Dataset name (without @snapshot part)
   * @param {array} properties - Properties to retrieve
   */
  async SnapshotListForDataset(datasetName, properties = []) {
    const filters = [["dataset", "=", datasetName]];
    return await this.SnapshotList(filters, properties);
  }

  /**
   * List snapshots for a dataset and all children (recursive)
   * @param {string} parentDatasetName - Parent dataset name
   * @param {array} properties - Properties to retrieve
   */
  async SnapshotListRecursive(parentDatasetName, properties = []) {
    const filters = [["dataset", "^", parentDatasetName]];
    return await this.SnapshotList(filters, properties);
  }

  /**
   * Destroy snapshots matching criteria
   * @param {string} datasetName - Dataset name
   * @param {object} data - Snapshot destruction criteria
   */
  async DatasetDestroySnapshots(datasetName, data = {}) {
    await this.call("pool.dataset.destroy_snapshots", [datasetName, data]);
  }

  // ============================================================================
  // SNAPSHOT OPERATIONS
  // ============================================================================

  /**
   * Create a snapshot
   * @param {string} snapshotName - Full snapshot name (dataset@snapshot)
   * @param {object} data - Snapshot options
   */
  async SnapshotCreate(snapshotName, data = {}) {
    const parts = snapshotName.split("@");
    if (parts.length !== 2) {
      throw new Error(`Invalid snapshot name: ${snapshotName}`);
    }

    const params = {
      dataset: parts[0],
      name: parts[1],
      ...data,
    };

    await this.call("zfs.snapshot.create", [params]);
  }

  /**
   * Delete a snapshot
   * @param {string} snapshotName - Full snapshot name (dataset@snapshot)
   * @param {object} data - Delete options
   */
  async SnapshotDelete(snapshotName, data = {}) {
    try {
      await this.call("zfs.snapshot.delete", [snapshotName, data]);
    } catch (error) {
      // Ignore "does not exist" errors
      if (error.message && error.message.includes("does not exist")) {
        return;
      }
      throw error;
    }
  }

  /**
   * Update snapshot properties
   * @param {string} snapshotName - Full snapshot name
   * @param {object} properties - Properties to update
   */
  async SnapshotSet(snapshotName, properties) {
    const params = {
      ...this.getSystemProperties(properties),
      user_properties_update: this.getPropertiesKeyValueArray(
        this.getUserProperties(properties)
      ),
    };

    await this.call("zfs.snapshot.update", [snapshotName, params]);
  }

  /**
   * Get snapshot properties
   * @param {string} snapshotName - Full snapshot name
   * @param {array} properties - Specific properties to retrieve (optional)
   */
  async SnapshotGet(snapshotName, properties = []) {
    const filters = [["id", "=", snapshotName]];
    const options = {};

    if (properties && properties.length > 0) {
      options.select = properties;
    }

    const results = await this.query("zfs.snapshot.query", filters, options);

    if (!results || results.length === 0) {
      throw new Error(`Snapshot not found: ${snapshotName}`);
    }

    return results[0];
  }

  // ============================================================================
  // CLONE OPERATIONS
  // ============================================================================

  /**
   * Clone a snapshot to create a new dataset
   * @param {string} snapshotName - Source snapshot name
   * @param {string} datasetName - Target dataset name
   * @param {object} data - Clone options
   */
  async CloneCreate(snapshotName, datasetName, data = {}) {
    const params = {
      snapshot: snapshotName,
      dataset_dst: datasetName,
      ...data,
    };

    await this.call("zfs.snapshot.clone", [params]);
  }

  // ============================================================================
  // REPLICATION
  // ============================================================================

  /**
   * Run a one-time replication task
   * @param {object} data - Replication configuration
   */
  async ReplicationRunOnetime(data) {
    const jobId = await this.call("replication.run_onetime", [data]);
    return jobId;
  }

  // ============================================================================
  // JOB MANAGEMENT
  // ============================================================================

  /**
   * Wait for a job to complete
   * @param {number} jobId - Job ID to wait for
   * @param {number} timeout - Timeout in seconds (0 = no timeout)
   * @param {number} checkInterval - Interval between checks in milliseconds
   */
  async CoreWaitForJob(jobId, timeout = 0, checkInterval = 3000) {
    const startTime = Date.now();

    while (true) {
      const job = await this.call("core.get_jobs", [[["id", "=", jobId]]]);

      if (!job || job.length === 0) {
        throw new Error(`Job ${jobId} not found`);
      }

      const jobInfo = job[0];

      // Check job state
      if (jobInfo.state === "SUCCESS") {
        return jobInfo;
      } else if (jobInfo.state === "FAILED") {
        throw new Error(`Job ${jobId} failed: ${jobInfo.error || "Unknown error"}`);
      } else if (jobInfo.state === "ABORTED") {
        throw new Error(`Job ${jobId} was aborted`);
      }

      // Check timeout
      if (timeout > 0) {
        const elapsed = (Date.now() - startTime) / 1000;
        if (elapsed >= timeout) {
          throw new Error(`Job ${jobId} timed out after ${timeout} seconds`);
        }
      }

      // Wait before checking again
      await sleep(checkInterval);
    }
  }

  /**
   * Get jobs matching filters
   * @param {array} filters - Job filters
   */
  async CoreGetJobs(filters = []) {
    return await this.call("core.get_jobs", [filters]);
  }

  // ============================================================================
  // FILESYSTEM PERMISSIONS
  // ============================================================================

  /**
   * Set filesystem permissions
   * @param {object} data - Permission data (path, mode, uid, gid, options)
   */
  async FilesystemSetperm(data) {
    const jobId = await this.call("filesystem.setperm", [data]);
    return jobId;
  }

  /**
   * Change filesystem ownership
   * @param {object} data - Ownership data (path, uid, gid, options)
   */
  async FilesystemChown(data) {
    await this.call("filesystem.chown", [data]);
  }

  // ============================================================================
  // PROPERTY HELPERS
  // ============================================================================

  /**
   * Check if a property is a user property
   */
  getIsUserProperty(property) {
    return property.includes(":");
  }

  /**
   * Split properties into system and user properties
   */
  getSystemProperties(properties) {
    const systemProps = {};
    for (const [key, value] of Object.entries(properties)) {
      if (!this.getIsUserProperty(key)) {
        systemProps[key] = value;
      }
    }
    return systemProps;
  }

  /**
   * Get only user properties
   */
  getUserProperties(properties) {
    const userProps = {};
    for (const [key, value] of Object.entries(properties)) {
      if (this.getIsUserProperty(key)) {
        userProps[key] = value;
      }
    }
    return userProps;
  }

  /**
   * Convert properties object to key-value array format
   */
  getPropertiesKeyValueArray(properties) {
    return Object.entries(properties).map(([key, value]) => ({
      key,
      value: String(value),
    }));
  }

  /**
   * Normalize properties from a dataset/snapshot object
   * Extracts only specified properties and normalizes them to expected format
   * @param {object} item - Dataset or snapshot object from API
   * @param {array} properties - List of property names to extract
   * @returns {object} - Normalized properties object
   */
  normalizeProperties(item, properties = []) {
    const result = {};

    for (const prop of properties) {
      // Check if property is a user property (contains ':')
      const isUserProp = prop.includes(":");

      if (isUserProp) {
        // User properties are in user_properties object
        if (item.user_properties && item.user_properties[prop]) {
          result[prop] = {
            value: item.user_properties[prop].value,
            rawvalue: item.user_properties[prop].rawvalue || item.user_properties[prop].value,
            source: item.user_properties[prop].source || "local",
          };
        } else {
          result[prop] = {
            value: "-",
            rawvalue: "-",
            source: "none",
          };
        }
      } else {
        // System properties - check various locations
        if (item[prop] !== undefined) {
          // Property might be a direct value or an object with value/rawvalue
          if (typeof item[prop] === "object" && item[prop] !== null) {
            result[prop] = {
              value: item[prop].value !== undefined ? item[prop].value : item[prop].parsed,
              rawvalue: item[prop].rawvalue !== undefined ? item[prop].rawvalue : item[prop].value,
              source: item[prop].source || "local",
            };
          } else {
            result[prop] = {
              value: item[prop],
              rawvalue: item[prop],
              source: "local",
            };
          }
        } else if (item.properties && item.properties[prop]) {
          // Check in nested properties object (for snapshots)
          result[prop] = {
            value: item.properties[prop].value,
            rawvalue: item.properties[prop].rawvalue || item.properties[prop].value,
            source: item.properties[prop].source || "local",
          };
        } else {
          result[prop] = {
            value: "-",
            rawvalue: "-",
            source: "none",
          };
        }
      }
    }

    return result;
  }

  // ============================================================================
  // NFS SHARE MANAGEMENT
  // ============================================================================

  /**
   * Create an NFS share
   * @param {object} data - NFS share configuration
   */
  async NFSShareCreate(data) {
    return await this.call("sharing.nfs.create", [data]);
  }

  /**
   * Query NFS shares
   * @param {array} filters - Query filters
   */
  async NFSShareQuery(filters = []) {
    return await this.query("sharing.nfs.query", filters);
  }

  /**
   * Update an NFS share
   * @param {number} id - Share ID
   * @param {object} data - Updated configuration
   */
  async NFSShareUpdate(id, data) {
    return await this.call("sharing.nfs.update", [id, data]);
  }

  /**
   * Delete an NFS share
   * @param {number} id - Share ID
   */
  async NFSShareDelete(id) {
    try {
      await this.call("sharing.nfs.delete", [id]);
    } catch (error) {
      // Ignore "does not exist" errors
      if (error.message && error.message.includes("does not exist")) {
        return;
      }
      throw error;
    }
  }

  /**
   * Find NFS share by path
   * @param {string} path - Mount path to search for
   */
  async NFSShareFindByPath(path) {
    return await this.findResourceByProperties("sharing.nfs.query", (item) => {
      return item.path === path || (item.paths && item.paths.includes(path));
    });
  }

  // ============================================================================
  // SMB SHARE MANAGEMENT (deprecated - will be removed)
  // ============================================================================

  /**
   * SMB is not supported in this TrueNAS-only version
   * Use NFS or iSCSI instead
   */
  async SMBShareCreate(data) {
    throw new Error("SMB shares are not supported. Use NFS or iSCSI instead.");
  }

  // ============================================================================
  // iSCSI MANAGEMENT
  // ============================================================================

  /**
   * Query iSCSI targets
   * @param {array} filters - Query filters
   */
  async ISCSITargetQuery(filters = []) {
    return await this.query("iscsi.target.query", filters);
  }

  /**
   * Create an iSCSI target
   * @param {object} data - Target configuration
   */
  async ISCSITargetCreate(data) {
    return await this.call("iscsi.target.create", [data]);
  }

  /**
   * Update an iSCSI target
   * @param {number} id - Target ID
   * @param {object} data - Updated configuration
   */
  async ISCSITargetUpdate(id, data) {
    return await this.call("iscsi.target.update", [id, data]);
  }

  /**
   * Delete an iSCSI target
   * @param {number} id - Target ID
   * @param {boolean} force - Force deletion
   */
  async ISCSITargetDelete(id, force = false) {
    try {
      await this.call("iscsi.target.delete", [id, force]);
    } catch (error) {
      // Ignore "does not exist" errors
      if (error.message && error.message.includes("does not exist")) {
        return;
      }
      throw error;
    }
  }

  /**
   * Query iSCSI extents
   * @param {array} filters - Query filters
   */
  async ISCSIExtentQuery(filters = []) {
    return await this.query("iscsi.extent.query", filters);
  }

  /**
   * Create an iSCSI extent
   * @param {object} data - Extent configuration
   */
  async ISCSIExtentCreate(data) {
    return await this.call("iscsi.extent.create", [data]);
  }

  /**
   * Update an iSCSI extent
   * @param {number} id - Extent ID
   * @param {object} data - Updated configuration
   */
  async ISCSIExtentUpdate(id, data) {
    return await this.call("iscsi.extent.update", [id, data]);
  }

  /**
   * Delete an iSCSI extent
   * @param {number} id - Extent ID
   * @param {boolean} remove - Remove underlying file/zvol
   * @param {boolean} force - Force deletion
   */
  async ISCSIExtentDelete(id, remove = false, force = false) {
    try {
      await this.call("iscsi.extent.delete", [id, remove, force]);
    } catch (error) {
      // Ignore "does not exist" errors
      if (error.message && error.message.includes("does not exist")) {
        return;
      }
      throw error;
    }
  }

  /**
   * Query iSCSI target-to-extent associations
   * @param {array} filters - Query filters
   */
  async ISCSITargetExtentQuery(filters = []) {
    return await this.query("iscsi.targetextent.query", filters);
  }

  /**
   * Create an iSCSI target-to-extent association
   * @param {object} data - Association configuration
   */
  async ISCSITargetExtentCreate(data) {
    return await this.call("iscsi.targetextent.create", [data]);
  }

  /**
   * Update an iSCSI target-to-extent association
   * @param {number} id - Association ID
   * @param {object} data - Updated configuration
   */
  async ISCSITargetExtentUpdate(id, data) {
    return await this.call("iscsi.targetextent.update", [id, data]);
  }

  /**
   * Delete an iSCSI target-to-extent association
   * @param {number} id - Association ID
   * @param {boolean} force - Force deletion
   */
  async ISCSITargetExtentDelete(id, force = false) {
    try {
      await this.call("iscsi.targetextent.delete", [id, force]);
    } catch (error) {
      // Ignore "does not exist" errors
      if (error.message && error.message.includes("does not exist")) {
        return;
      }
      throw error;
    }
  }

  /**
   * Query iSCSI portals
   * @param {array} filters - Query filters
   */
  async ISCSIPortalQuery(filters = []) {
    return await this.query("iscsi.portal.query", filters);
  }

  /**
   * Query iSCSI initiators
   * @param {array} filters - Query filters
   */
  async ISCSIInitiatorQuery(filters = []) {
    return await this.query("iscsi.initiator.query", filters);
  }

  /**
   * Get iSCSI global configuration
   */
  async ISCSIGlobalConfigGet() {
    return await this.call("iscsi.global.config");
  }

  /**
   * Update iSCSI global configuration
   * @param {object} data - Configuration updates
   */
  async ISCSIGlobalConfigUpdate(data) {
    return await this.call("iscsi.global.update", [data]);
  }

  // ============================================================================
  // NVMe-oF MANAGEMENT (TrueNAS SCALE 25.10+ uses nvmet.* namespace)
  // ============================================================================

  /**
   * Query NVMe-oF subsystems
   * TrueNAS API: nvmet.subsys.query
   * @param {array} filters - Query filters
   */
  async NVMeOFSubsystemQuery(filters = []) {
    return await this.query("nvmet.subsys.query", filters);
  }

  /**
   * Create an NVMe-oF subsystem
   * TrueNAS API: nvmet.subsys.create
   * @param {object} data - Subsystem configuration
   */
  async NVMeOFSubsystemCreate(data) {
    return await this.call("nvmet.subsys.create", [data]);
  }

  /**
   * Update an NVMe-oF subsystem
   * TrueNAS API: nvmet.subsys.update
   * @param {number} id - Subsystem ID
   * @param {object} data - Updated configuration
   */
  async NVMeOFSubsystemUpdate(id, data) {
    return await this.call("nvmet.subsys.update", [id, data]);
  }

  /**
   * Delete an NVMe-oF subsystem
   * TrueNAS API: nvmet.subsys.delete
   * @param {number} id - Subsystem ID
   */
  async NVMeOFSubsystemDelete(id) {
    try {
      await this.call("nvmet.subsys.delete", [id]);
    } catch (error) {
      // Ignore "does not exist" errors
      if (error.message && error.message.includes("does not exist")) {
        return;
      }
      throw error;
    }
  }

  /**
   * Query NVMe-oF namespaces
   * TrueNAS API: nvmet.namespace.query
   * @param {array} filters - Query filters
   */
  async NVMeOFNamespaceQuery(filters = []) {
    return await this.query("nvmet.namespace.query", filters);
  }

  /**
   * Create an NVMe-oF namespace
   * TrueNAS API: nvmet.namespace.create
   * @param {object} data - Namespace configuration
   */
  async NVMeOFNamespaceCreate(data) {
    return await this.call("nvmet.namespace.create", [data]);
  }

  /**
   * Update an NVMe-oF namespace
   * TrueNAS API: nvmet.namespace.update
   * @param {number} id - Namespace ID
   * @param {object} data - Updated configuration
   */
  async NVMeOFNamespaceUpdate(id, data) {
    return await this.call("nvmet.namespace.update", [id, data]);
  }

  /**
   * Delete an NVMe-oF namespace
   * TrueNAS API: nvmet.namespace.delete
   * @param {number} id - Namespace ID
   */
  async NVMeOFNamespaceDelete(id) {
    try {
      await this.call("nvmet.namespace.delete", [id]);
    } catch (error) {
      // Ignore "does not exist" errors
      if (error.message && error.message.includes("does not exist")) {
        return;
      }
      throw error;
    }
  }

  /**
   * Query NVMe-oF hosts
   * TrueNAS API: nvmet.host.query
   * @param {array} filters - Query filters
   */
  async NVMeOFHostQuery(filters = []) {
    return await this.query("nvmet.host.query", filters);
  }

  /**
   * Create an NVMe-oF host
   * TrueNAS API: nvmet.host.create
   * @param {object} data - Host configuration
   */
  async NVMeOFHostCreate(data) {
    return await this.call("nvmet.host.create", [data]);
  }

  /**
   * Delete an NVMe-oF host
   * TrueNAS API: nvmet.host.delete
   * @param {number} id - Host ID
   */
  async NVMeOFHostDelete(id) {
    try {
      await this.call("nvmet.host.delete", [id]);
    } catch (error) {
      // Ignore "does not exist" errors
      if (error.message && error.message.includes("does not exist")) {
        return;
      }
      throw error;
    }
  }

  /**
   * Query NVMe-oF ports
   * TrueNAS API: nvmet.port.query
   * @param {array} filters - Query filters
   */
  async NVMeOFPortQuery(filters = []) {
    return await this.query("nvmet.port.query", filters);
  }

  /**
   * Get NVMe-oF listener addresses (transport address choices)
   * TrueNAS API: nvmet.port.transport_address_choices
   * @param {string} transport - Transport type: 'TCP' or 'RDMA'
   */
  async NVMeOFGetListenerAddresses(transport = "TCP") {
    return await this.call("nvmet.port.transport_address_choices", [transport]);
  }
}

module.exports.Api = Api;
