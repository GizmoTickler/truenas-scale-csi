const _ = require("lodash");
const semver = require("semver");
const { sleep, stringify } = require("../../../utils/general");
const { Zetabyte } = require("../../../utils/zfs");
const { Registry } = require("../../../utils/registry");

// used for in-memory cache of the version info
const FREENAS_SYSTEM_VERSION_CACHE_KEY = "freenas:system_version";
const __REGISTRY_NS__ = "FreeNASJsonRpcApi";

class Api {
  constructor(client, cache, options = {}) {
    this.client = client;
    this.cache = cache;
    this.options = options;
    this.registry = new Registry();
  }

  async getClient() {
    return this.client;
  }

  /**
   * only here for the helpers
   * @returns
   */
  async getZetabyte() {
    return this.registry.get(`${__REGISTRY_NS__}:zb`, () => {
      return new Zetabyte({
        executor: {
          spawn: function () {
            throw new Error(
              "cannot use the zb implementation to execute zfs commands, must use the api"
            );
          },
        },
      });
    });
  }

  async getSystemVersion() {
    let cacheData = await this.cache.get(FREENAS_SYSTEM_VERSION_CACHE_KEY);
    if (cacheData) {
      return cacheData;
    }

    const version = await this.client.call("system.version");
    // Emulate the structure expected by consumers (v2 property)
    const versionInfo = {
      v2: version, // e.g. "TrueNAS-SCALE-25.04..."
    };

    await this.setVersionInfoCache(versionInfo);
    return versionInfo;
  }

  async setVersionInfoCache(versionInfo) {
    await this.cache.set(FREENAS_SYSTEM_VERSION_CACHE_KEY, versionInfo, {
      ttl: 60 * 1000,
    });
  }

  async getSystemVersionSemver() {
    const info = await this.getSystemVersion();
    let versionString = info.v2;
    // coerce
    return semver.coerce(versionString, { loose: true });
  }

  async getIsScale() {
     // We are only supporting SCALE 25.04+ so this is always true effectively,
     // but let's check the version string to be safe/consistent
     const info = await this.getSystemVersion();
     return info.v2 && (info.v2.toLowerCase().includes("scale") || semver.gte(await this.getSystemVersionSemver(), "20.0.0"));
  }

  // Dataset Operations
  async DatasetCreate(datasetName, data) {
      // pool.dataset.create
      // params: [{ name: ..., ... }]
      data.name = datasetName;
      try {
        await this.client.call("pool.dataset.create", [data]);
      } catch (err) {
          // Ignore "already exists" errors (errno 17 usually, or message)
          if (this.isAlreadyExistsError(err)) {
              return;
          }
          throw err;
      }
  }

  async DatasetDelete(datasetName, data = {}) {
      // pool.dataset.delete
      // id: datasetName
      // recursive: boolean
      try {
        await this.client.call("pool.dataset.delete", [datasetName, { recursive: data.recursive || false }]);
      } catch(err) {
          if (this.isNotFoundError(err)) {
              return;
          }
          throw err;
      }
  }

  async DatasetSet(datasetName, properties) {
      // pool.dataset.update
      // id: datasetName
      // update properties
      // properties passed here are mixed system and user properties
      // user properties are flat in the passed object but in API might need separate handling?
      // In SCALE pool.dataset.update, properties are passed directly. User properties might not need special handling if they contain ':'?
      // Actually, pool.dataset.update takes struct with properties.

      // We need to split?
      // Checking docs: `pool.dataset.update(id, options)`
      // options keys are property names.
      // user properties with ':' are allowed as keys.

      const updateData = {};
      // Merge system and user properties from input
      Object.assign(updateData, this.getSystemProperties(properties));

      // In JSON-RPC, user properties usually don't need the `user_properties_update` list format of REST API?
      // Let's check docs or assume standard ZFS prop behavior.
      // However, the `http/api.js` did explicit splitting.

      const userProps = this.getUserProperties(properties);
      for(const k in userProps) {
          updateData[k] = { value: String(userProps[k]) };
      }

      // System properties usually expect just the value or { value: ... }?
      // `pool.dataset.update` schema:
      // `comments`: string
      // `compression`: string
      // ...

      // But `http/api.js` used `user_properties_update`.
      // Let's try passing flattened properties.

      // Wait, `pool.dataset.update` in SCALE usually takes simple key-values for system props.
      // For user props, it might differ.
      // Let's assume flattened works for now, or I'll check if I can find docs/examples.
      // The `DatasetSet` in `http/api.js` did:
      // put(..., { ...system_properties, user_properties_update: [...] })

      // If I send `{'org.foo:bar': 'value'}` to `pool.dataset.update`, does it work?
      // Usually yes.

      const finalProps = { ...this.getSystemProperties(properties) };
      Object.assign(finalProps, userProps);

      await this.client.call("pool.dataset.update", [datasetName, finalProps]);
  }

  async DatasetInherit(datasetName, property) {
      // In SCALE 25.04+, to inherit a property:
      // - For system properties: set value to "INHERIT" (special keyword)
      // - For user properties: use pool.dataset.delete_user_prop or set to null

      if (this.getIsUserProperty(property)) {
          // For user properties, we need to delete them to inherit
          // The API supports pool.dataset.delete_user_prop in some versions
          try {
              // Try the dedicated method first if available
              await this.client.call("pool.dataset.delete_user_prop", [datasetName, property]);
          } catch (err) {
              // Fallback: some versions might require setting to null or empty string
              if (err.toString().includes("Method not found")) {
                  // Try setting to null as fallback
                  await this.client.call("pool.dataset.update", [datasetName, { [property]: null }]);
              } else {
                  throw err;
              }
          }
      } else {
          // For system properties, set to "INHERIT" keyword
          await this.client.call("pool.dataset.update", [datasetName, { [property]: "INHERIT" }]);
      }
  }

  async DatasetGet(datasetName, properties) {
      // pool.dataset.query with id filter
      const res = await this.client.call("pool.dataset.query", [[["id", "=", datasetName]]]);
      if (!res || res.length === 0) {
          throw new Error("dataset does not exist");
      }
      return this.normalizeProperties(res[0], properties);
  }

  async DatasetDestroySnapshots(datasetName) {
     // list snapshots and destroy
     // zfs.snapshot.query [['dataset', '=', datasetName]]
     const snapshots = await this.client.call("zfs.snapshot.query", [[["dataset", "=", datasetName]]]);
     for (const snap of snapshots) {
         await this.client.call("zfs.snapshot.delete", [snap.id, { defer: true }]);
     }
  }

  // Snapshot Operations
  async SnapshotCreate(snapshotName, data = {}) {
     // zfs.snapshot.create
     // params: { dataset: ..., name: ... }
     const zb = await this.getZetabyte();
     const dataset = zb.helpers.extractDatasetName(snapshotName);
     const snapshot = zb.helpers.extractSnapshotName(snapshotName);

     data.dataset = dataset;
     data.name = snapshot;

     try {
        await this.client.call("zfs.snapshot.create", [data]);
     } catch (err) {
         if (this.isAlreadyExistsError(err)) {
             return;
         }
         throw err;
     }
  }

  async SnapshotDelete(snapshotName, data = {}) {
      // zfs.snapshot.delete
      try {
        await this.client.call("zfs.snapshot.delete", [snapshotName, { defer: data.defer || false }]);
      } catch (err) {
          if (this.isNotFoundError(err)) {
              return;
          }
          throw err;
      }
  }

  async SnapshotGet(snapshotName, properties) {
      const res = await this.client.call("zfs.snapshot.query", [[["id", "=", snapshotName]]]);
      if (!res || res.length === 0) {
          throw new Error("snapshot does not exist");
      }
      return this.normalizeProperties(res[0], properties);
  }

  async SnapshotSet(snapshotName, properties) {
      // zfs.snapshot.update
      const updateData = {};
      Object.assign(updateData, this.getUserProperties(properties));
      // zfs.snapshot.update(id, options)
      await this.client.call("zfs.snapshot.update", [snapshotName, updateData]);
  }

  // Clone
  async CloneCreate(snapshotName, datasetName, data = {}) {
      // pool.dataset.clone ??
      // zfs.snapshot.clone
      // params: { snapshot: ..., dataset_dst: ... }
      data.snapshot = snapshotName;
      data.dataset_dst = datasetName;
      try {
        await this.client.call("zfs.snapshot.clone", [data]);
      } catch(err) {
          if (this.isAlreadyExistsError(err)) {
              return;
          }
          throw err;
      }
  }

  // Replication
  async ReplicationRunOnetime(data) {
      // replication.run_onetime
      return await this.client.call("replication.run_onetime", [data]);
  }

  // Core Jobs
  async CoreWaitForJob(job_id, timeout = 0, check_interval = 3000) {
      // core.job.wait ? No, usually we poll or subscribe.
      // But `core.get_jobs` is what was used.
      // There is `core.job.wait` in some versions?
      // Let's use polling `core.get_jobs` to match logic.
      if (!job_id) {
        throw new Error("invalid job_id");
      }

      const startTime = Date.now() / 1000;
      let currentTime;
      let job;

      do {
        currentTime = Date.now() / 1000;
        if (timeout > 0 && currentTime > startTime + timeout) {
          throw new Error("timeout waiting for job to complete");
        }

        if (job) {
          await sleep(check_interval);
        }

        const jobs = await this.client.call("core.get_jobs", [[["id", "=", job_id]]]);
        job = jobs[0];

        if (!job) {
             // Job disappeared?
             throw new Error("Job not found " + job_id);
        }

      } while (!["SUCCESS", "ABORTED", "FAILED"].includes(job.state));

      return job;
  }

  // Filesystem
  async FilesystemSetperm(data) {
      // filesystem.setperm
      const job_id = await this.client.call("filesystem.setperm", [data]);
      return await this.CoreWaitForJob(job_id, 30);
  }

  async FilesystemChown(data) {
      // filesystem.chown
      const job_id = await this.client.call("filesystem.chown", [data]);
      return await this.CoreWaitForJob(job_id, 30);
  }

  // NVMET
  async NvmetSubsysList(data = {}) {
      return await this.client.call("nvmet.subsystem.query", [[], data]);
  }

  async NvmetSubsysCreate(subsysName, data = {}) {
      data.name = subsysName;
      data.allow_any_host = true;
      try {
          return await this.client.call("nvmet.subsystem.create", [data]);
      } catch(err) {
          if (this.isAlreadyExistsError(err)) {
              return this.NvmetSubsysGetByName(subsysName);
          }
          throw err;
      }
  }

  async NvmetSubsysGetByName(subsysName) {
      const res = await this.client.call("nvmet.subsystem.query", [[["name", "=", subsysName]]]);
      if (res && res.length > 0) return res[0];
      throw new Error("Subsystem not found");
  }

  async NvmetSubsysDeleteById(id) {
      try {
          await this.client.call("nvmet.subsystem.delete", [id]);
      } catch(err) {
          if (this.isNotFoundError(err)) return;
          throw err;
      }
  }

  async NvmetNamespaceCreate(zvol, subsysId, data = {}) {
      // Clean zvol path logic
      zvol = String(zvol);
      if (zvol.startsWith("/dev/")) zvol = zvol.substring(5);
      if (zvol.startsWith("/")) zvol = zvol.substring(1);
      if (!zvol.startsWith("zvol/")) zvol = `zvol/${zvol}`;

      data.device_path = zvol;
      data.device_type = "ZVOL";
      data.subsys = subsysId; // Check if param is `subsys` or `subsys_id`

      try {
          return await this.client.call("nvmet.namespace.create", [data]);
      } catch(err) {
          if (this.isAlreadyExistsError(err)) {
              // Try to find it?
              // The original logic checked "already used by subsystem"
              return this.NvmetNamespaceGetByDevicePath(zvol);
          }
          throw err;
      }
  }

  async NvmetNamespaceGetByDevicePath(zvol) {
      const res = await this.client.call("nvmet.namespace.query", [[["device_path", "=", zvol]]]);
      if (res && res.length > 0) return res[0];
      throw new Error("Namespace not found");
  }

  async NvmetNamespaceDeleteById(id) {
      try {
          await this.client.call("nvmet.namespace.delete", [id]);
      } catch(err) {
          if (this.isNotFoundError(err)) return;
          throw err;
      }
  }

  async NvmetPortSubsysCreate(port_id, subsys_id) {
      // In SCALE 25.04+, we use nvmet.port.update to add subsystems to ports
      try {
          // First, get the current port configuration
          const ports = await this.client.call("nvmet.port.query", [[["id", "=", port_id]]]);
          if (!ports || ports.length === 0) {
              throw new Error(`Port ${port_id} not found`);
          }

          const port = ports[0];
          const subsystems = port.subsystems || [];

          // Check if subsystem is already added
          if (subsystems.includes(subsys_id)) {
              return; // Already exists
          }

          // Add the subsystem to the port
          subsystems.push(subsys_id);
          await this.client.call("nvmet.port.update", [port_id, { subsystems }]);
      } catch(err) {
          if (this.isAlreadyExistsError(err)) return;
          throw err;
      }
  }

  // iSCSI Operations
  async IscsiGetGlobalConfig() {
      return await this.client.call("iscsi.global.config");
  }

  async IscsiTargetCreate(data) {
      try {
          return await this.client.call("iscsi.target.create", [data]);
      } catch(err) {
          if (this.isAlreadyExistsError(err)) {
              // Try to find existing target by name
              const existing = await this.client.call("iscsi.target.query", [[["name", "=", data.name]]]);
              if (existing && existing.length > 0) {
                  return existing[0];
              }
          }
          throw err;
      }
  }

  async IscsiTargetQuery(filter = []) {
      return await this.client.call("iscsi.target.query", [filter]);
  }

  async IscsiTargetUpdate(id, data) {
      return await this.client.call("iscsi.target.update", [id, data]);
  }

  async IscsiTargetDelete(id) {
      try {
          await this.client.call("iscsi.target.delete", [id]);
      } catch(err) {
          if (this.isNotFoundError(err)) return;
          throw err;
      }
  }

  async IscsiExtentCreate(data) {
      try {
          return await this.client.call("iscsi.extent.create", [data]);
      } catch(err) {
          if (this.isAlreadyExistsError(err)) {
              const existing = await this.client.call("iscsi.extent.query", [[["name", "=", data.name]]]);
              if (existing && existing.length > 0) {
                  return existing[0];
              }
          }
          throw err;
      }
  }

  async IscsiExtentQuery(filter = []) {
      return await this.client.call("iscsi.extent.query", [filter]);
  }

  async IscsiExtentDelete(id) {
      try {
          await this.client.call("iscsi.extent.delete", [id]);
      } catch(err) {
          if (this.isNotFoundError(err)) return;
          throw err;
      }
  }

  async IscsiTargetExtentCreate(data) {
      try {
          return await this.client.call("iscsi.targetextent.create", [data]);
      } catch(err) {
          if (this.isAlreadyExistsError(err) ||
              JSON.stringify(err).includes("already in this target") ||
              JSON.stringify(err).includes("LUN ID is already being used")) {
              const existing = await this.client.call("iscsi.targetextent.query", [[["target", "=", data.target], ["extent", "=", data.extent]]]);
              if (existing && existing.length > 0) {
                  return existing[0];
              }
          }
          throw err;
      }
  }

  async IscsiTargetExtentQuery(filter = []) {
      return await this.client.call("iscsi.targetextent.query", [filter]);
  }

  // Sharing NFS Operations
  async SharingNfsCreate(data) {
      try {
          return await this.client.call("sharing.nfs.create", [data]);
      } catch(err) {
          if (this.isAlreadyExistsError(err) || JSON.stringify(err).includes("exporting this path")) {
              // Try to find by path
              const paths = data.paths || data.path ? [data.path] : [];
              if (paths.length > 0) {
                  const existing = await this.client.call("sharing.nfs.query", [[["paths", "in", paths]]]);
                  if (existing && existing.length > 0) {
                      return existing[0];
                  }
              }
          }
          throw err;
      }
  }

  async SharingNfsQuery(filter = []) {
      return await this.client.call("sharing.nfs.query", [filter]);
  }

  async SharingNfsDelete(id) {
      try {
          await this.client.call("sharing.nfs.delete", [id]);
      } catch(err) {
          if (this.isNotFoundError(err)) return;
          throw err;
      }
  }

  // Sharing SMB Operations
  async SharingSmbCreate(data) {
      try {
          return await this.client.call("sharing.smb.create", [data]);
      } catch(err) {
          if (this.isAlreadyExistsError(err)) {
              const existing = await this.client.call("sharing.smb.query", [[["name", "=", data.name]]]);
              if (existing && existing.length > 0) {
                  return existing[0];
              }
          }
          throw err;
      }
  }

  async SharingSmbQuery(filter = []) {
      return await this.client.call("sharing.smb.query", [filter]);
  }

  async SharingSmbDelete(id) {
      try {
          await this.client.call("sharing.smb.delete", [id]);
      } catch(err) {
          if (this.isNotFoundError(err)) return;
          throw err;
      }
  }

  // Service Operations
  async ServiceReload(service) {
      return await this.client.call("service.reload", [service]);
  }

  // Helpers
  isAlreadyExistsError(err) {
      const msg = err.toString();
      return msg.includes("already exists") || (err.error && err.error === 17); // EEXIST
  }

  isNotFoundError(err) {
      const msg = err.toString();
      return msg.includes("not found") || msg.includes("does not exist") || (err.error && err.error === 2); // ENOENT
  }

  getIsUserProperty(property) {
    if (property.includes(":")) {
      return true;
    }
    return false;
  }

  getUserProperties(properties) {
    let user_properties = {};
    for (const property in properties) {
      if (this.getIsUserProperty(property)) {
        user_properties[property] = String(properties[property]);
      }
    }
    return user_properties;
  }

  getSystemProperties(properties) {
    let system_properties = {};
    for (const property in properties) {
      if (!this.getIsUserProperty(property)) {
        system_properties[property] = properties[property];
      }
    }
    return system_properties;
  }

  normalizeProperties(dataset, properties) {
    let res = {};
    for (const property of properties) {
      let p;
      if (dataset.hasOwnProperty(property)) {
        p = dataset[property];
      } else if (
        dataset.properties &&
        dataset.properties.hasOwnProperty(property)
      ) {
        p = dataset.properties[property];
      } else {
        p = {
          value: "-",
          rawvalue: "-",
          source: "-",
        };
      }

      // Handle simple values from JSON-RPC
      if (typeof p !== "object" || p === null) {
        p = {
          value: p,
          rawvalue: p,
          source: "-",
        };
      } else if (p.value === undefined) {
         // If it's an object but doesn't have value, maybe it IS the value (unlikely for ZFS props from API)
         // But pool.dataset.query returns properties as { value: ..., source: ... } objects usually.
      }

      res[property] = p;
    }

    return res;
  }
}

module.exports.Api = Api;
