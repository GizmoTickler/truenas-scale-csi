const _ = require("lodash");
const { GrpcError, grpc } = require("../../utils/grpc");
const { CsiBaseDriver } = require("../index");
const HttpClient = require("./http").Client;
const TrueNASApiClient = require("./http/api").Api;
const { Zetabyte } = require("../../utils/zfs");
const GeneralUtils = require("../../utils/general");

const Handlebars = require("handlebars");
const uuidv4 = require("uuid").v4;
const semver = require("semver");

// TrueNAS SCALE share properties
const TRUENAS_NFS_SHARE_PROPERTY_NAME = "democratic-csi:truenas_nfs_share_id";
const TRUENAS_ISCSI_TARGET_ID_PROPERTY_NAME =
  "democratic-csi:truenas_iscsi_target_id";
const TRUENAS_ISCSI_EXTENT_ID_PROPERTY_NAME =
  "democratic-csi:truenas_iscsi_extent_id";
const TRUENAS_ISCSI_TARGETTOEXTENT_ID_PROPERTY_NAME =
  "democratic-csi:truenas_iscsi_targetextent_id";
const TRUENAS_NVMEOF_SUBSYSTEM_ID_PROPERTY_NAME =
  "democratic-csi:truenas_nvmeof_subsystem_id";
const TRUENAS_NVMEOF_NAMESPACE_ID_PROPERTY_NAME =
  "democratic-csi:truenas_nvmeof_namespace_id";

// zfs common properties
const MANAGED_PROPERTY_NAME = "democratic-csi:managed_resource";
const SUCCESS_PROPERTY_NAME = "democratic-csi:provision_success";
const VOLUME_SOURCE_CLONE_SNAPSHOT_PREFIX = "volume-source-for-volume-";
const VOLUME_SOURCE_DETACHED_SNAPSHOT_PREFIX = "volume-source-for-snapshot-";
const VOLUME_CSI_NAME_PROPERTY_NAME = "democratic-csi:csi_volume_name";
const SHARE_VOLUME_CONTEXT_PROPERTY_NAME =
  "democratic-csi:csi_share_volume_context";
const VOLUME_CONTENT_SOURCE_TYPE_PROPERTY_NAME =
  "democratic-csi:csi_volume_content_source_type";
const VOLUME_CONTENT_SOURCE_ID_PROPERTY_NAME =
  "democratic-csi:csi_volume_content_source_id";
const SNAPSHOT_CSI_NAME_PROPERTY_NAME = "democratic-csi:csi_snapshot_name";
const SNAPSHOT_CSI_SOURCE_VOLUME_ID_PROPERTY_NAME =
  "democratic-csi:csi_snapshot_source_volume_id";

const VOLUME_CONTEXT_PROVISIONER_DRIVER_PROPERTY_NAME =
  "democratic-csi:volume_context_provisioner_driver";
const VOLUME_CONTEXT_PROVISIONER_INSTANCE_ID_PROPERTY_NAME =
  "democratic-csi:volume_context_provisioner_instance_id";

const __REGISTRY_NS__ = "TrueNASApiDriver";

class TrueNASApiDriver extends CsiBaseDriver {
  constructor(ctx, options) {
    super(...arguments);

    options = options || {};
    options.service = options.service || {};
    options.service.identity = options.service.identity || {};
    options.service.controller = options.service.controller || {};
    options.service.node = options.service.node || {};

    options.service.identity.capabilities =
      options.service.identity.capabilities || {};

    options.service.controller.capabilities =
      options.service.controller.capabilities || {};

    options.service.node.capabilities = options.service.node.capabilities || {};

    if (!("service" in options.service.identity.capabilities)) {
      this.ctx.logger.debug("setting default identity service caps");

      options.service.identity.capabilities.service = [
        //"UNKNOWN",
        "CONTROLLER_SERVICE",
        //"VOLUME_ACCESSIBILITY_CONSTRAINTS"
      ];
    }

    if (!("volume_expansion" in options.service.identity.capabilities)) {
      this.ctx.logger.debug("setting default identity volume_expansion caps");

      options.service.identity.capabilities.volume_expansion = [
        //"UNKNOWN",
        "ONLINE",
        //"OFFLINE"
      ];
    }

    if (!("rpc" in options.service.controller.capabilities)) {
      this.ctx.logger.debug("setting default controller caps");

      options.service.controller.capabilities.rpc = [
        //"UNKNOWN",
        "CREATE_DELETE_VOLUME",
        //"PUBLISH_UNPUBLISH_VOLUME",
        //"LIST_VOLUMES_PUBLISHED_NODES",
        "LIST_VOLUMES",
        "GET_CAPACITY",
        "CREATE_DELETE_SNAPSHOT",
        "LIST_SNAPSHOTS",
        "CLONE_VOLUME",
        //"PUBLISH_READONLY",
        "EXPAND_VOLUME",
      ];

      if (semver.satisfies(this.ctx.csiVersion, ">=1.3.0")) {
        options.service.controller.capabilities.rpc.push(
          //"VOLUME_CONDITION",
          "GET_VOLUME"
        );
      }

      if (semver.satisfies(this.ctx.csiVersion, ">=1.5.0")) {
        options.service.controller.capabilities.rpc.push(
          "SINGLE_NODE_MULTI_WRITER"
        );
      }
    }

    if (!("rpc" in options.service.node.capabilities)) {
      this.ctx.logger.debug("setting default node caps");

      switch (this.getDriverZfsResourceType()) {
        case "filesystem":
          options.service.node.capabilities.rpc = [
            //"UNKNOWN",
            "STAGE_UNSTAGE_VOLUME",
            "GET_VOLUME_STATS",
            //"EXPAND_VOLUME",
          ];
          break;
        case "volume":
          options.service.node.capabilities.rpc = [
            //"UNKNOWN",
            "STAGE_UNSTAGE_VOLUME",
            "GET_VOLUME_STATS",
            "EXPAND_VOLUME",
          ];
          break;
      }

      if (semver.satisfies(this.ctx.csiVersion, ">=1.3.0")) {
        //options.service.node.capabilities.rpc.push("VOLUME_CONDITION");
      }

      if (semver.satisfies(this.ctx.csiVersion, ">=1.5.0")) {
        options.service.node.capabilities.rpc.push("SINGLE_NODE_MULTI_WRITER");
        /**
         * This is for volumes that support a mount time gid such as smb or fat
         */
        //options.service.node.capabilities.rpc.push("VOLUME_MOUNT_GROUP");
      }
    }
  }

  /**
   * only here for the helpers
   * @returns
   */
  async getZetabyte() {
    return this.ctx.registry.get(`${__REGISTRY_NS__}:zb`, () => {
      return new Zetabyte({
        executor: {
          spawn: function () {
            throw new Error(
              "cannot use the zb implementation to execute zfs commands, must use the http api"
            );
          },
        },
      });
    });
  }

  /**
   * should create any necessary share resources
   * should set the SHARE_VOLUME_CONTEXT_PROPERTY_NAME propery
   *
   * @param {*} datasetName
  /**
   * Create share resources for a dataset (NFS, iSCSI, or NVMe-oF)
   * TrueNAS SCALE 25.04+ using WebSocket JSON-RPC API
   *
   * @param {*} call - gRPC call context
   * @param {string} datasetName - Full dataset name
   * @returns {object} volume_context - Connection information for the volume
   */
  async createShare(call, datasetName) {
    const driverShareType = this.getDriverShareType();
    const httpApiClient = await this.getTrueNASHttpApiClient();
    const zb = await this.getZetabyte();

    this.ctx.logger.info(`Creating ${driverShareType} share for dataset: ${datasetName}`);

    let volume_context;

    switch (driverShareType) {
      case "nfs":
        volume_context = await this.createNFSShare(call, datasetName, httpApiClient, zb);
        break;
      case "iscsi":
        volume_context = await this.createISCSIShare(call, datasetName, httpApiClient, zb);
        break;
      case "nvmeof":
        volume_context = await this.createNVMeOFShare(call, datasetName, httpApiClient, zb);
        break;
      default:
        throw new GrpcError(
          grpc.status.FAILED_PRECONDITION,
          `Unsupported share type: ${driverShareType}. Only nfs, iscsi, and nvmeof are supported.`
        );
    }

    return volume_context;
  }

  /**
   * Create NFS share for a dataset
   */
  async createNFSShare(call, datasetName, httpApiClient, zb) {
    // Get dataset properties
    const properties = await httpApiClient.DatasetGet(datasetName, [
      "mountpoint",
      TRUENAS_NFS_SHARE_PROPERTY_NAME,
    ]);

    this.ctx.logger.debug("Dataset properties: %j", properties);

    const mountpoint = properties.mountpoint.value;
    const shareId = properties[TRUENAS_NFS_SHARE_PROPERTY_NAME].value;

    // Check if share already exists
    if (!zb.helpers.isPropertyValueSet(shareId)) {
      // Generate share comment
      let nfsShareComment;
      if (this.options.nfs.shareCommentTemplate) {
        nfsShareComment = Handlebars.compile(
          this.options.nfs.shareCommentTemplate
        )({
          name: call.request.name,
          parameters: call.request.parameters,
          csi: {
            name: this.ctx.args.csiName,
            version: this.ctx.args.csiVersion,
          },
          zfs: {
            datasetName: datasetName,
          },
        });
      } else {
        nfsShareComment = `democratic-csi (${this.ctx.args.csiName}): ${datasetName}`;
      }

      // Create NFS share
      const shareConfig = {
        path: mountpoint,
        comment: nfsShareComment || "",
        networks: this.options.nfs.shareAllowedNetworks || [],
        hosts: this.options.nfs.shareAllowedHosts || [],
        ro: false,
        maproot_user: this.options.nfs.shareMaprootUser || null,
        maproot_group: this.options.nfs.shareMaprootGroup || null,
        mapall_user: this.options.nfs.shareMapallUser || null,
        mapall_group: this.options.nfs.shareMapallGroup || null,
        security: [],
      };

      this.ctx.logger.debug("Creating NFS share: %j", shareConfig);

      try {
        const share = await httpApiClient.NFSShareCreate(shareConfig);
        this.ctx.logger.info(`NFS share created with ID: ${share.id}`);

        // Store share ID in ZFS property
        await httpApiClient.DatasetSet(datasetName, {
          [TRUENAS_NFS_SHARE_PROPERTY_NAME]: share.id,
        });
      } catch (error) {
        // Check if share already exists for this path
        if (error.message && error.message.includes("already exports")) {
          this.ctx.logger.warn("NFS share already exists, finding existing share");
          const existingShare = await httpApiClient.NFSShareFindByPath(mountpoint);

          if (!existingShare) {
            throw new GrpcError(
              grpc.status.UNKNOWN,
              `Failed to find existing NFS share for path: ${mountpoint}`
            );
          }

          // Store existing share ID
          await httpApiClient.DatasetSet(datasetName, {
            [TRUENAS_NFS_SHARE_PROPERTY_NAME]: existingShare.id,
          });
        } else {
          throw new GrpcError(
            grpc.status.UNKNOWN,
            `Failed to create NFS share: ${error.message}`
          );
        }
      }
    }

    // Return volume context for NFS
    return {
      node_attach_driver: "nfs",
      server: this.options.nfs.shareHost,
      share: mountpoint,
    };
  }

  /**
   * Create iSCSI share (target, extent, and target-extent association)
   */
  async createISCSIShare(call, datasetName, httpApiClient, zb) {
    // Get dataset properties
    const properties = await httpApiClient.DatasetGet(datasetName, [
      "name",
      TRUENAS_ISCSI_TARGET_ID_PROPERTY_NAME,
      TRUENAS_ISCSI_EXTENT_ID_PROPERTY_NAME,
      TRUENAS_ISCSI_TARGETTOEXTENT_ID_PROPERTY_NAME,
    ]);

    const targetId = properties[TRUENAS_ISCSI_TARGET_ID_PROPERTY_NAME].value;
    const extentId = properties[TRUENAS_ISCSI_EXTENT_ID_PROPERTY_NAME].value;
    const targetExtentId = properties[TRUENAS_ISCSI_TARGETTOEXTENT_ID_PROPERTY_NAME].value;

    // Check if already fully configured
    if (zb.helpers.isPropertyValueSet(targetExtentId)) {
      this.ctx.logger.debug("iSCSI assets already exist");
    } else {
      // Generate iSCSI name
      let iscsiName;
      if (this.options.iscsi.nameTemplate) {
        iscsiName = Handlebars.compile(this.options.iscsi.nameTemplate)({
          name: call.request.name,
          parameters: call.request.parameters,
        });
      } else {
        iscsiName = zb.helpers.extractLeafName(datasetName);
      }

      if (this.options.iscsi.namePrefix) {
        iscsiName = this.options.iscsi.namePrefix + iscsiName;
      }
      if (this.options.iscsi.nameSuffix) {
        iscsiName += this.options.iscsi.nameSuffix;
      }

      this.ctx.logger.info(`Creating iSCSI assets with name: ${iscsiName}`);

      // Get global iSCSI configuration for basename
      const globalConfig = await httpApiClient.ISCSIGlobalConfigGet();
      const basename = globalConfig.basename;
      this.ctx.logger.debug(`iSCSI basename: ${basename}`);

      // Create or find target
      let target;
      if (!zb.helpers.isPropertyValueSet(targetId)) {
        const targetConfig = {
          name: iscsiName,
          alias: this.options.iscsi.targetAlias || "",
          mode: this.options.iscsi.targetMode || "ISCSI",
          groups: this.options.iscsi.targetGroups || [],
        };

        try {
          target = await httpApiClient.ISCSITargetCreate(targetConfig);
          this.ctx.logger.info(`iSCSI target created with ID: ${target.id}`);

          await httpApiClient.DatasetSet(datasetName, {
            [TRUENAS_ISCSI_TARGET_ID_PROPERTY_NAME]: target.id,
          });
        } catch (error) {
          if (error.message && error.message.includes("already exists")) {
            this.ctx.logger.warn("iSCSI target already exists, finding it");
            const targets = await httpApiClient.ISCSITargetQuery([["name", "=", iscsiName]]);
            if (targets && targets.length > 0) {
              target = targets[0];
              await httpApiClient.DatasetSet(datasetName, {
                [TRUENAS_ISCSI_TARGET_ID_PROPERTY_NAME]: target.id,
              });
            } else {
              throw new GrpcError(
                grpc.status.UNKNOWN,
                `Failed to find existing iSCSI target: ${iscsiName}`
              );
            }
          } else {
            throw error;
          }
        }
      }

      // Create or find extent
      let extent;
      if (!zb.helpers.isPropertyValueSet(extentId)) {
        const extentDiskName = `zvol/${datasetName}`;

        let extentComment;
        if (this.options.iscsi.extentCommentTemplate) {
          extentComment = Handlebars.compile(
            this.options.iscsi.extentCommentTemplate
          )({
            name: call.request.name,
            parameters: call.request.parameters,
            csi: {
              name: this.ctx.args.csiName,
              version: this.ctx.args.csiVersion,
            },
            zfs: {
              datasetName: datasetName,
            },
          });
        } else {
          extentComment = `democratic-csi: ${datasetName}`;
        }

        const extentConfig = {
          name: iscsiName,
          type: "DISK",
          disk: extentDiskName,
          comment: extentComment,
          insecure_tpc: this.options.iscsi.extentInsecureTpc !== false,
          xen: this.options.iscsi.extentXenCompat || false,
          blocksize: this.options.iscsi.extentBlocksize || 512,
          pblocksize: !this.options.iscsi.extentDisablePhysicalBlocksize,
          rpm: this.options.iscsi.extentRpm || "SSD",
          ro: false,
        };

        if (this.options.iscsi.extentAvailThreshold > 0 &&
            this.options.iscsi.extentAvailThreshold <= 100) {
          extentConfig.avail_threshold = this.options.iscsi.extentAvailThreshold;
        }

        try {
          extent = await httpApiClient.ISCSIExtentCreate(extentConfig);
          this.ctx.logger.info(`iSCSI extent created with ID: ${extent.id}`);

          await httpApiClient.DatasetSet(datasetName, {
            [TRUENAS_ISCSI_EXTENT_ID_PROPERTY_NAME]: extent.id,
          });
        } catch (error) {
          if (error.message && error.message.includes("already exists")) {
            this.ctx.logger.warn("iSCSI extent already exists, finding it");
            const extents = await httpApiClient.ISCSIExtentQuery([["name", "=", iscsiName]]);
            if (extents && extents.length > 0) {
              extent = extents[0];
              await httpApiClient.DatasetSet(datasetName, {
                [TRUENAS_ISCSI_EXTENT_ID_PROPERTY_NAME]: extent.id,
              });
            } else {
              throw new GrpcError(
                grpc.status.UNKNOWN,
                `Failed to find existing iSCSI extent: ${iscsiName}`
              );
            }
          } else {
            throw error;
          }
        }
      }

      // Create target-to-extent association
      if (!zb.helpers.isPropertyValueSet(targetExtentId)) {
        // Get current target and extent IDs from properties
        const currentProps = await httpApiClient.DatasetGet(datasetName, [
          TRUENAS_ISCSI_TARGET_ID_PROPERTY_NAME,
          TRUENAS_ISCSI_EXTENT_ID_PROPERTY_NAME,
        ]);

        const finalTargetId = currentProps[TRUENAS_ISCSI_TARGET_ID_PROPERTY_NAME].value;
        const finalExtentId = currentProps[TRUENAS_ISCSI_EXTENT_ID_PROPERTY_NAME].value;

        const targetExtentConfig = {
          target: parseInt(finalTargetId),
          extent: parseInt(finalExtentId),
          lunid: this.options.iscsi.targetExtentLunid || null,
        };

        const targetExtent = await httpApiClient.ISCSITargetExtentCreate(targetExtentConfig);
        this.ctx.logger.info(`iSCSI target-extent created with ID: ${targetExtent.id}`);

        await httpApiClient.DatasetSet(datasetName, {
          [TRUENAS_ISCSI_TARGETTOEXTENT_ID_PROPERTY_NAME]: targetExtent.id,
        });
      }
    }

    // Get final target name for volume context
    const finalProps = await httpApiClient.DatasetGet(datasetName, [
      TRUENAS_ISCSI_TARGET_ID_PROPERTY_NAME,
    ]);
    const finalTargetId = finalProps[TRUENAS_ISCSI_TARGET_ID_PROPERTY_NAME].value;
    const targets = await httpApiClient.ISCSITargetQuery([["id", "=", parseInt(finalTargetId)]]);
    const targetName = targets[0].name;

    // Get global config for IQN
    const globalConfig = await httpApiClient.ISCSIGlobalConfigGet();
    const iqn = `${globalConfig.basename}:${targetName}`;

    // Return volume context for iSCSI
    return {
      node_attach_driver: "iscsi",
      portal: this.options.iscsi.targetPortal,
      iqn: iqn,
      lun: this.options.iscsi.targetExtentLunid || "0",
      interface: this.options.iscsi.interface || "default",
    };
  }

  /**
   * Create NVMe-oF share (subsystem and namespace)
   */
  async createNVMeOFShare(call, datasetName, httpApiClient, zb) {
    // Get dataset properties
    const properties = await httpApiClient.DatasetGet(datasetName, [
      "name",
      TRUENAS_NVMEOF_SUBSYSTEM_ID_PROPERTY_NAME,
      TRUENAS_NVMEOF_NAMESPACE_ID_PROPERTY_NAME,
    ]);

    const subsystemId = properties[TRUENAS_NVMEOF_SUBSYSTEM_ID_PROPERTY_NAME].value;
    const namespaceId = properties[TRUENAS_NVMEOF_NAMESPACE_ID_PROPERTY_NAME].value;

    // Check if already fully configured
    if (zb.helpers.isPropertyValueSet(namespaceId)) {
      this.ctx.logger.debug("NVMe-oF assets already exist");
    } else {
      // Generate NVMe-oF name
      let nvmeofName;
      if (this.options.nvmeof.nameTemplate) {
        nvmeofName = Handlebars.compile(this.options.nvmeof.nameTemplate)({
          name: call.request.name,
          parameters: call.request.parameters,
        });
      } else {
        nvmeofName = zb.helpers.extractLeafName(datasetName);
      }

      if (this.options.nvmeof.namePrefix) {
        nvmeofName = this.options.nvmeof.namePrefix + nvmeofName;
      }
      if (this.options.nvmeof.nameSuffix) {
        nvmeofName += this.options.nvmeof.nameSuffix;
      }

      this.ctx.logger.info(`Creating NVMe-oF assets with name: ${nvmeofName}`);

      // Create or find subsystem
      let subsystem;
      if (!zb.helpers.isPropertyValueSet(subsystemId)) {
        const subsystemConfig = {
          nqn: nvmeofName,
          serial: uuidv4().replace(/-/g, '').substring(0, 20),
          hosts: this.options.nvmeof.subsystemHosts || [],
          allow_any_host: this.options.nvmeof.subsystemAllowAnyHost !== false,
        };

        try {
          subsystem = await httpApiClient.NVMeOFSubsystemCreate(subsystemConfig);
          this.ctx.logger.info(`NVMe-oF subsystem created with ID: ${subsystem.id}`);

          await httpApiClient.DatasetSet(datasetName, {
            [TRUENAS_NVMEOF_SUBSYSTEM_ID_PROPERTY_NAME]: subsystem.id,
          });
        } catch (error) {
          if (error.message && error.message.includes("already exists")) {
            this.ctx.logger.warn("NVMe-oF subsystem already exists, finding it");
            const subsystems = await httpApiClient.NVMeOFSubsystemQuery([["nqn", "=", nvmeofName]]);
            if (subsystems && subsystems.length > 0) {
              subsystem = subsystems[0];
              await httpApiClient.DatasetSet(datasetName, {
                [TRUENAS_NVMEOF_SUBSYSTEM_ID_PROPERTY_NAME]: subsystem.id,
              });
            } else {
              throw new GrpcError(
                grpc.status.UNKNOWN,
                `Failed to find existing NVMe-oF subsystem: ${nvmeofName}`
              );
            }
          } else {
            throw error;
          }
        }
      }

      // Create namespace
      if (!zb.helpers.isPropertyValueSet(namespaceId)) {
        const currentProps = await httpApiClient.DatasetGet(datasetName, [
          TRUENAS_NVMEOF_SUBSYSTEM_ID_PROPERTY_NAME,
        ]);
        const finalSubsystemId = currentProps[TRUENAS_NVMEOF_SUBSYSTEM_ID_PROPERTY_NAME].value;

        const namespacePath = `/dev/zvol/${datasetName}`;

        const namespaceConfig = {
          subsystem: parseInt(finalSubsystemId),
          path: namespacePath,
          nsid: this.options.nvmeof.namespaceNsid || null,
        };

        const namespace = await httpApiClient.NVMeOFNamespaceCreate(namespaceConfig);
        this.ctx.logger.info(`NVMe-oF namespace created with ID: ${namespace.id}`);

        await httpApiClient.DatasetSet(datasetName, {
          [TRUENAS_NVMEOF_NAMESPACE_ID_PROPERTY_NAME]: namespace.id,
        });
      }
    }

    // Get final subsystem NQN for volume context
    const finalProps = await httpApiClient.DatasetGet(datasetName, [
      TRUENAS_NVMEOF_SUBSYSTEM_ID_PROPERTY_NAME,
    ]);
    const finalSubsystemId = finalProps[TRUENAS_NVMEOF_SUBSYSTEM_ID_PROPERTY_NAME].value;
    const subsystems = await httpApiClient.NVMeOFSubsystemQuery([["id", "=", parseInt(finalSubsystemId)]]);
    const subsystemNqn = subsystems[0].nqn;

    // Get listener addresses
    const listenerAddresses = await httpApiClient.NVMeOFGetListenerAddresses();
    const portal = listenerAddresses && listenerAddresses.length > 0
      ? listenerAddresses[0]
      : this.options.nvmeof.targetPortal;

    // Return volume context for NVMe-oF
    return {
      node_attach_driver: "nvmeof",
      portal: portal,
      nqn: subsystemNqn,
      transport: this.options.nvmeof.transport || "tcp",
    };
  }

  /**
   * Delete share resources for a dataset
   * TrueNAS SCALE 25.04+ using WebSocket JSON-RPC API
   *
   * @param {*} call - gRPC call context
   * @param {string} datasetName - Full dataset name
   */
  async deleteShare(call, datasetName) {
    const driverShareType = this.getDriverShareType();
    const httpApiClient = await this.getTrueNASHttpApiClient();
    const zb = await this.getZetabyte();

    this.ctx.logger.info(`Deleting ${driverShareType} share for dataset: ${datasetName}`);

    try {
      switch (driverShareType) {
        case "nfs":
          await this.deleteNFSShare(datasetName, httpApiClient, zb);
          break;
        case "iscsi":
          await this.deleteISCSIShare(datasetName, httpApiClient, zb);
          break;
        case "nvmeof":
          await this.deleteNVMeOFShare(datasetName, httpApiClient, zb);
          break;
        default:
          throw new GrpcError(
            grpc.status.FAILED_PRECONDITION,
            `Unsupported share type: ${driverShareType}`
          );
      }
    } catch (error) {
      // If dataset doesn't exist, that's fine
      if (error.message && error.message.includes("does not exist")) {
        this.ctx.logger.debug("Dataset or share already deleted");
        return;
      }
      throw error;
    }
  }

  /**
   * Delete NFS share
   */
  async deleteNFSShare(datasetName, httpApiClient, zb) {
    try {
      const properties = await httpApiClient.DatasetGet(datasetName, [
        TRUENAS_NFS_SHARE_PROPERTY_NAME,
      ]);

      const shareId = properties[TRUENAS_NFS_SHARE_PROPERTY_NAME].value;

      if (zb.helpers.isPropertyValueSet(shareId)) {
        this.ctx.logger.debug(`Deleting NFS share ID: ${shareId}`);
        await httpApiClient.NFSShareDelete(parseInt(shareId));

        // Remove property
        await httpApiClient.DatasetInherit(
          datasetName,
          TRUENAS_NFS_SHARE_PROPERTY_NAME
        );
      }
    } catch (error) {
      if (error.message && error.message.includes("does not exist")) {
        return;
      }
      throw error;
    }
  }

  /**
   * Delete iSCSI share (target-extent, extent, target)
   */
  async deleteISCSIShare(datasetName, httpApiClient, zb) {
    try {
      const properties = await httpApiClient.DatasetGet(datasetName, [
        TRUENAS_ISCSI_TARGET_ID_PROPERTY_NAME,
        TRUENAS_ISCSI_EXTENT_ID_PROPERTY_NAME,
        TRUENAS_ISCSI_TARGETTOEXTENT_ID_PROPERTY_NAME,
      ]);

      const targetId = properties[TRUENAS_ISCSI_TARGET_ID_PROPERTY_NAME].value;
      const extentId = properties[TRUENAS_ISCSI_EXTENT_ID_PROPERTY_NAME].value;
      const targetExtentId = properties[TRUENAS_ISCSI_TARGETTOEXTENT_ID_PROPERTY_NAME].value;

      // Delete target-extent association first
      if (zb.helpers.isPropertyValueSet(targetExtentId)) {
        this.ctx.logger.debug(`Deleting iSCSI target-extent ID: ${targetExtentId}`);
        await httpApiClient.ISCSITargetExtentDelete(parseInt(targetExtentId), true);
        await httpApiClient.DatasetInherit(
          datasetName,
          TRUENAS_ISCSI_TARGETTOEXTENT_ID_PROPERTY_NAME
        );
      }

      // Delete extent
      if (zb.helpers.isPropertyValueSet(extentId)) {
        this.ctx.logger.debug(`Deleting iSCSI extent ID: ${extentId}`);
        await httpApiClient.ISCSIExtentDelete(parseInt(extentId), false, true);
        await httpApiClient.DatasetInherit(
          datasetName,
          TRUENAS_ISCSI_EXTENT_ID_PROPERTY_NAME
        );
      }

      // Delete target
      if (zb.helpers.isPropertyValueSet(targetId)) {
        this.ctx.logger.debug(`Deleting iSCSI target ID: ${targetId}`);
        await httpApiClient.ISCSITargetDelete(parseInt(targetId), true);
        await httpApiClient.DatasetInherit(
          datasetName,
          TRUENAS_ISCSI_TARGET_ID_PROPERTY_NAME
        );
      }
    } catch (error) {
      if (error.message && error.message.includes("does not exist")) {
        return;
      }
      throw error;
    }
  }

  /**
   * Delete NVMe-oF share (namespace and subsystem)
   */
  async deleteNVMeOFShare(datasetName, httpApiClient, zb) {
    try {
      const properties = await httpApiClient.DatasetGet(datasetName, [
        TRUENAS_NVMEOF_SUBSYSTEM_ID_PROPERTY_NAME,
        TRUENAS_NVMEOF_NAMESPACE_ID_PROPERTY_NAME,
      ]);

      const subsystemId = properties[TRUENAS_NVMEOF_SUBSYSTEM_ID_PROPERTY_NAME].value;
      const namespaceId = properties[TRUENAS_NVMEOF_NAMESPACE_ID_PROPERTY_NAME].value;

      // Delete namespace first
      if (zb.helpers.isPropertyValueSet(namespaceId)) {
        this.ctx.logger.debug(`Deleting NVMe-oF namespace ID: ${namespaceId}`);
        await httpApiClient.NVMeOFNamespaceDelete(parseInt(namespaceId));
        await httpApiClient.DatasetInherit(
          datasetName,
          TRUENAS_NVMEOF_NAMESPACE_ID_PROPERTY_NAME
        );
      }

      // Delete subsystem
      if (zb.helpers.isPropertyValueSet(subsystemId)) {
        this.ctx.logger.debug(`Deleting NVMe-oF subsystem ID: ${subsystemId}`);
        await httpApiClient.NVMeOFSubsystemDelete(parseInt(subsystemId));
        await httpApiClient.DatasetInherit(
          datasetName,
          TRUENAS_NVMEOF_SUBSYSTEM_ID_PROPERTY_NAME
        );
      }
    } catch (error) {
      if (error.message && error.message.includes("does not exist")) {
        return;
      }
      throw error;
    }
  }

  async removeSnapshotsFromDatatset(datasetName) {
    const httpApiClient = await this.getTrueNASHttpApiClient();
    let job_id = await httpApiClient.DatasetDestroySnapshots(datasetName);
    await httpApiClient.CoreWaitForJob(job_id, 30);
  }

  /**
   * Expand volume - not needed for TrueNAS SCALE 25.04+
   * The middleware automatically handles volume expansion
   *
   * @param {*} call
   * @param {*} datasetName
   * @returns
   */
  async expandVolume(call, datasetName) {
    // TrueNAS SCALE middleware automatically reloads configuration
    // No manual reload needed
    return;
  }

  async getVolumeStatus(volume_id) {
    const driver = this;

    if (!!!semver.satisfies(driver.ctx.csiVersion, ">=1.2.0")) {
      return;
    }

    let abnormal = false;
    let message = "OK";
    let volume_status = {};

    //LIST_VOLUMES_PUBLISHED_NODES
    if (
      semver.satisfies(driver.ctx.csiVersion, ">=1.2.0") &&
      driver.options.service.controller.capabilities.rpc.includes(
        "LIST_VOLUMES_PUBLISHED_NODES"
      )
    ) {
      // TODO: let drivers fill this in
      volume_status.published_node_ids = [];
    }

    //VOLUME_CONDITION
    if (
      semver.satisfies(driver.ctx.csiVersion, ">=1.3.0") &&
      driver.options.service.controller.capabilities.rpc.includes(
        "VOLUME_CONDITION"
      )
    ) {
      // TODO: let drivers fill ths in
      volume_condition = { abnormal, message };
      volume_status.volume_condition = volume_condition;
    }

    return volume_status;
  }

  async populateCsiVolumeFromData(row) {
    const driver = this;
    const zb = await this.getZetabyte();
    const driverZfsResourceType = this.getDriverZfsResourceType();
    let datasetName = this.getVolumeParentDatasetName();

    // ignore rows were csi_name is empty
    if (row[MANAGED_PROPERTY_NAME] != "true") {
      return;
    }

    if (
      !zb.helpers.isPropertyValueSet(row[SHARE_VOLUME_CONTEXT_PROPERTY_NAME])
    ) {
      driver.ctx.logger.warn(`${row.name} is missing share context`);
      return;
    }

    let volume_content_source;
    let volume_context = JSON.parse(row[SHARE_VOLUME_CONTEXT_PROPERTY_NAME]);
    if (
      zb.helpers.isPropertyValueSet(
        row[VOLUME_CONTEXT_PROVISIONER_DRIVER_PROPERTY_NAME]
      )
    ) {
      volume_context["provisioner_driver"] =
        row[VOLUME_CONTEXT_PROVISIONER_DRIVER_PROPERTY_NAME];
    }

    if (
      zb.helpers.isPropertyValueSet(
        row[VOLUME_CONTEXT_PROVISIONER_INSTANCE_ID_PROPERTY_NAME]
      )
    ) {
      volume_context["provisioner_driver_instance_id"] =
        row[VOLUME_CONTEXT_PROVISIONER_INSTANCE_ID_PROPERTY_NAME];
    }

    if (
      zb.helpers.isPropertyValueSet(
        row[VOLUME_CONTENT_SOURCE_TYPE_PROPERTY_NAME]
      )
    ) {
      volume_content_source = {};
      switch (row[VOLUME_CONTENT_SOURCE_TYPE_PROPERTY_NAME]) {
        case "snapshot":
          volume_content_source.snapshot = {};
          volume_content_source.snapshot.snapshot_id =
            row[VOLUME_CONTENT_SOURCE_ID_PROPERTY_NAME];
          break;
        case "volume":
          volume_content_source.volume = {};
          volume_content_source.volume.volume_id =
            row[VOLUME_CONTENT_SOURCE_ID_PROPERTY_NAME];
          break;
      }
    }

    let volume = {
      // remove parent dataset info
      volume_id: row["name"].replace(new RegExp("^" + datasetName + "/"), ""),
      capacity_bytes:
        driverZfsResourceType == "filesystem"
          ? row["refquota"]
          : row["volsize"],
      content_source: volume_content_source,
      volume_context,
    };

    return volume;
  }

  /**
   * cannot make this a storage class parameter as storage class/etc context is *not* sent
   * into various calls such as GetControllerCapabilities etc
   */
  getDriverZfsResourceType() {
    switch (this.options.driver) {
      case "truenas-nfs":
        return "filesystem";
      case "truenas-iscsi":
      case "truenas-nvmeof":
        return "volume";
      default:
        throw new Error("unknown driver: " + this.ctx.args.driver);
    }
  }

  getDriverShareType() {
    switch (this.options.driver) {
      case "truenas-nfs":
        return "nfs";
      case "truenas-iscsi":
        return "iscsi";
      case "truenas-nvmeof":
        return "nvmeof";
      default:
        throw new Error("unknown driver: " + this.ctx.args.driver);
    }
  }

  getDatasetParentName() {
    let datasetParentName = this.options.zfs.datasetParentName;
    datasetParentName = datasetParentName.replace(/\/$/, "");
    return datasetParentName;
  }

  getVolumeParentDatasetName() {
    let datasetParentName = this.getDatasetParentName();
    //datasetParentName += "/v";
    datasetParentName = datasetParentName.replace(/\/$/, "");
    return datasetParentName;
  }

  getDetachedSnapshotParentDatasetName() {
    //let datasetParentName = this.getDatasetParentName();
    let datasetParentName = this.options.zfs.detachedSnapshotsDatasetParentName;
    //datasetParentName += "/s";
    datasetParentName = datasetParentName.replace(/\/$/, "");
    return datasetParentName;
  }

  async getHttpClient() {
    return this.ctx.registry.get(`${__REGISTRY_NS__}:http_client`, () => {
      const client = new HttpClient(this.options.httpConnection);
      client.logger = this.ctx.logger;
      client.setApiVersion(2); // requires version 2
      return client;
    });
  }

  async getMinimumVolumeSize() {
    const driverZfsResourceType = this.getDriverZfsResourceType();
    switch (driverZfsResourceType) {
      case "filesystem":
        return 1073741824;
    }
  }

  async getTrueNASHttpApiClient() {
    return this.ctx.registry.getAsync(`${__REGISTRY_NS__}:api_client`, async () => {
      const httpClient = await this.getHttpClient();
      return new TrueNASApiClient(httpClient, this.ctx.cache);
    });
  }

  getAccessModes(capability) {
    let access_modes = _.get(this.options, "csi.access_modes", null);
    if (access_modes !== null) {
      return access_modes;
    }

    const driverZfsResourceType = this.getDriverZfsResourceType();
    switch (driverZfsResourceType) {
      case "filesystem":
        access_modes = [
          "UNKNOWN",
          "SINGLE_NODE_WRITER",
          "SINGLE_NODE_SINGLE_WRITER", // added in v1.5.0
          "SINGLE_NODE_MULTI_WRITER", // added in v1.5.0
          "SINGLE_NODE_READER_ONLY",
          "MULTI_NODE_READER_ONLY",
          "MULTI_NODE_SINGLE_WRITER",
          "MULTI_NODE_MULTI_WRITER",
        ];
        break;
      case "volume":
        access_modes = [
          "UNKNOWN",
          "SINGLE_NODE_WRITER",
          "SINGLE_NODE_SINGLE_WRITER", // added in v1.5.0
          "SINGLE_NODE_MULTI_WRITER", // added in v1.5.0
          "SINGLE_NODE_READER_ONLY",
          "MULTI_NODE_READER_ONLY",
          "MULTI_NODE_SINGLE_WRITER",
        ];
        break;
    }

    if (
      capability.access_type == "block" &&
      !access_modes.includes("MULTI_NODE_MULTI_WRITER")
    ) {
      access_modes.push("MULTI_NODE_MULTI_WRITER");
    }

    return access_modes;
  }

  assertCapabilities(capabilities) {
    const driverZfsResourceType = this.getDriverZfsResourceType();
    this.ctx.logger.verbose("validating capabilities: %j", capabilities);

    let message = null;
    //[{"access_mode":{"mode":"SINGLE_NODE_WRITER"},"mount":{"mount_flags":["noatime","_netdev"],"fs_type":"nfs"},"access_type":"mount"}]
    const valid = capabilities.every((capability) => {
      switch (driverZfsResourceType) {
        case "filesystem":
          if (capability.access_type != "mount") {
            message = `invalid access_type ${capability.access_type}`;
            return false;
          }

          if (
            capability.mount.fs_type &&
            !["nfs", "cifs"].includes(capability.mount.fs_type)
          ) {
            message = `invalid fs_type ${capability.mount.fs_type}`;
            return false;
          }

          if (
            !this.getAccessModes(capability).includes(
              capability.access_mode.mode
            )
          ) {
            message = `invalid access_mode, ${capability.access_mode.mode}`;
            return false;
          }

          return true;
        case "volume":
          if (capability.access_type == "mount") {
            if (
              capability.mount.fs_type &&
              !GeneralUtils.default_supported_block_filesystems().includes(
                capability.mount.fs_type
              )
            ) {
              message = `invalid fs_type ${capability.mount.fs_type}`;
              return false;
            }
          }

          if (
            !this.getAccessModes(capability).includes(
              capability.access_mode.mode
            )
          ) {
            message = `invalid access_mode, ${capability.access_mode.mode}`;
            return false;
          }

          return true;
      }
    });

    return { valid, message };
  }

  /**
   * Get the max size a zvol name can be
   *
   * https://bugs.freebsd.org/bugzilla/show_bug.cgi?id=238112
   * https://svnweb.freebsd.org/base?view=revision&revision=343485
   * https://www.ixsystems.com/documentation/freenas/11.3-BETA1/intro.html#path-and-name-lengths
   */
  async getMaxZvolNameLength() {
    // TrueNAS SCALE 25.04+ uses scst with 255 character limit
    // https://github.com/dmeister/scst/blob/master/iscsi-scst/include/iscsi_scst.h#L28
    return 255;

    // Old legacy code below (unreachable)
    if (false) {
      return 63;
    }
  }

  /**
   * Ensure sane options are used etc
   * true = ready
   * false = not ready, but progressiong towards ready
   * throw error = faulty setup
   *
   * @param {*} call
   */
  async Probe(call) {
    const driver = this;
    const httpApiClient = await driver.getTrueNASHttpApiClient();

    if (driver.ctx.args.csiMode.includes("controller")) {
      let datasetParentName = this.getVolumeParentDatasetName() + "/";
      let snapshotParentDatasetName =
        this.getDetachedSnapshotParentDatasetName() + "/";
      if (
        datasetParentName.startsWith(snapshotParentDatasetName) ||
        snapshotParentDatasetName.startsWith(datasetParentName)
      ) {
        throw new GrpcError(
          grpc.status.FAILED_PRECONDITION,
          `datasetParentName and detachedSnapshotsDatasetParentName must not overlap`
        );
      }

      try {
        await httpApiClient.getSystemVersion();
      } catch (err) {
        throw new GrpcError(
          grpc.status.FAILED_PRECONDITION,
          `TrueNAS api is unavailable: ${String(err)}`
        );
      }

      // Driver only supports TrueNAS SCALE 25.04+
      // Version check is implicit through WebSocket API connection

      return super.Probe(...arguments);
    } else {
      return super.Probe(...arguments);
    }
  }

  /**
   * Create a volume doing in essence the following:
   * 1. create dataset
   * 2. create nfs share
   *
   * Should return 2 parameters
   * 1. `server` - host/ip of the nfs server
   * 2. `share` - path of the mount shared
   *
   * @param {*} call
   */
  async CreateVolume(call) {
    const driver = this;
    const driverZfsResourceType = this.getDriverZfsResourceType();
    const httpApiClient = await this.getTrueNASHttpApiClient();
    const zb = await this.getZetabyte();

    let datasetParentName = this.getVolumeParentDatasetName();
    let snapshotParentDatasetName = this.getDetachedSnapshotParentDatasetName();
    let zvolBlocksize = this.options.zfs.zvolBlocksize || "16K";
    let name = call.request.name;
    let volume_id = await driver.getVolumeIdFromCall(call);
    let volume_content_source = call.request.volume_content_source;
    let minimum_volume_size = await driver.getMinimumVolumeSize();
    let default_required_bytes = 1073741824;

    if (!datasetParentName) {
      throw new GrpcError(
        grpc.status.FAILED_PRECONDITION,
        `invalid configuration: missing datasetParentName`
      );
    }

    if (
      call.request.volume_capabilities &&
      call.request.volume_capabilities.length > 0
    ) {
      const result = this.assertCapabilities(call.request.volume_capabilities);
      if (result.valid !== true) {
        throw new GrpcError(grpc.status.INVALID_ARGUMENT, result.message);
      }
    } else {
      throw new GrpcError(
        grpc.status.INVALID_ARGUMENT,
        "missing volume_capabilities"
      );
    }

    // if no capacity_range specified set a required_bytes at least
    if (
      !call.request.capacity_range ||
      Object.keys(call.request.capacity_range).length === 0
    ) {
      call.request.capacity_range = {
        required_bytes: default_required_bytes,
      };
    }

    if (
      call.request.capacity_range.required_bytes > 0 &&
      call.request.capacity_range.limit_bytes > 0 &&
      call.request.capacity_range.required_bytes >
        call.request.capacity_range.limit_bytes
    ) {
      throw new GrpcError(
        grpc.status.OUT_OF_RANGE,
        `required_bytes is greather than limit_bytes`
      );
    }

    let capacity_bytes =
      call.request.capacity_range.required_bytes ||
      call.request.capacity_range.limit_bytes;

    if (!capacity_bytes) {
      //should never happen, value must be set
      throw new GrpcError(
        grpc.status.INVALID_ARGUMENT,
        `volume capacity is required (either required_bytes or limit_bytes)`
      );
    }

    // ensure *actual* capacity is not too small
    if (
      capacity_bytes > 0 &&
      minimum_volume_size > 0 &&
      capacity_bytes < minimum_volume_size
    ) {
      //throw new GrpcError(
      //  grpc.status.OUT_OF_RANGE,
      //  `volume capacity is smaller than the minimum: ${minimum_volume_size}`
      //);
      capacity_bytes = minimum_volume_size;
    }

    if (capacity_bytes && driverZfsResourceType == "volume") {
      //make sure to align capacity_bytes with zvol blocksize
      //volume size must be a multiple of volume block size
      capacity_bytes = zb.helpers.generateZvolSize(
        capacity_bytes,
        zvolBlocksize
      );
    }

    // ensure *actual* capacity is not greater than limit
    if (
      call.request.capacity_range.limit_bytes &&
      call.request.capacity_range.limit_bytes > 0 &&
      capacity_bytes > call.request.capacity_range.limit_bytes
    ) {
      throw new GrpcError(
        grpc.status.OUT_OF_RANGE,
        `required volume capacity is greater than limit`
      );
    }

    /**
     * NOTE: avoid the urge to templatize this given the name length limits for zvols
     * ie: namespace-name may quite easily exceed 58 chars
     */
    const datasetName = datasetParentName + "/" + volume_id;

    // ensure volumes with the same name being requested a 2nd time but with a different size fails
    try {
      let properties = await httpApiClient.DatasetGet(datasetName, [
        "volsize",
        "refquota",
      ]);
      let size;
      switch (driverZfsResourceType) {
        case "volume":
          size = properties["volsize"].rawvalue;
          break;
        case "filesystem":
          size = properties["refquota"].rawvalue;
          break;
        default:
          throw new Error(
            `unknown zfs resource type: ${driverZfsResourceType}`
          );
      }

      let check = false;
      if (driverZfsResourceType == "volume") {
        check = true;
      }

      if (
        driverZfsResourceType == "filesystem" &&
        this.options.zfs.datasetEnableQuotas
      ) {
        check = true;
      }

      if (check) {
        if (
          (call.request.capacity_range.required_bytes &&
            call.request.capacity_range.required_bytes > 0 &&
            size < call.request.capacity_range.required_bytes) ||
          (call.request.capacity_range.limit_bytes &&
            call.request.capacity_range.limit_bytes > 0 &&
            size > call.request.capacity_range.limit_bytes)
        ) {
          throw new GrpcError(
            grpc.status.ALREADY_EXISTS,
            `volume has already been created with a different size, existing size: ${size}, required_bytes: ${call.request.capacity_range.required_bytes}, limit_bytes: ${call.request.capacity_range.limit_bytes}`
          );
        }
      }
    } catch (err) {
      if (err.toString().includes("dataset does not exist")) {
        // does NOT already exist
      } else {
        throw err;
      }
    }

    /**
     * This is specifically a FreeBSD limitation, not sure what linux limit is
     * https://www.ixsystems.com/documentation/freenas/11.2-U5/storage.html#zfs-zvol-config-opts-tab
     * https://www.ixsystems.com/documentation/freenas/11.3-BETA1/intro.html#path-and-name-lengths
     * https://www.freebsd.org/cgi/man.cgi?query=devfs
     */
    if (driverZfsResourceType == "volume") {
      let extentDiskName = "zvol/" + datasetName;
      let maxZvolNameLength = await driver.getMaxZvolNameLength();
      driver.ctx.logger.debug("max zvol name length: %s", maxZvolNameLength);
      if (extentDiskName.length > maxZvolNameLength) {
        throw new GrpcError(
          grpc.status.FAILED_PRECONDITION,
          `extent disk name cannot exceed ${maxZvolNameLength} characters:  ${extentDiskName}`
        );
      }
    }

    let response, command;
    let volume_content_source_snapshot_id;
    let volume_content_source_volume_id;
    let fullSnapshotName;
    let volumeProperties = {};

    // user-supplied properties
    // put early to prevent stupid (user-supplied values overwriting system values)
    if (driver.options.zfs.datasetProperties) {
      for (let property in driver.options.zfs.datasetProperties) {
        let value = driver.options.zfs.datasetProperties[property];
        const template = Handlebars.compile(value);

        volumeProperties[property] = template({
          parameters: call.request.parameters,
        });
      }
    }

    volumeProperties[VOLUME_CSI_NAME_PROPERTY_NAME] = name;
    volumeProperties[MANAGED_PROPERTY_NAME] = "true";
    volumeProperties[VOLUME_CONTEXT_PROVISIONER_DRIVER_PROPERTY_NAME] =
      driver.options.driver;
    if (driver.options.instance_id) {
      volumeProperties[VOLUME_CONTEXT_PROVISIONER_INSTANCE_ID_PROPERTY_NAME] =
        driver.options.instance_id;
    }

    // TODO: also set access_mode as property?
    // TODO: also set fsType as property?

    // zvol enables reservation by default
    // this implements 'sparse' zvols
    let sparse;
    if (driverZfsResourceType == "volume") {
      // this is managed by the `sparse` option in the api
      if (!this.options.zfs.zvolEnableReservation) {
        volumeProperties.refreservation = 0;
      }
      sparse = Boolean(!this.options.zfs.zvolEnableReservation);
    }

    let detachedClone = false;

    // create dataset
    if (volume_content_source) {
      volumeProperties[VOLUME_CONTENT_SOURCE_TYPE_PROPERTY_NAME] =
        volume_content_source.type;
      switch (volume_content_source.type) {
        // must be available when adverstising CREATE_DELETE_SNAPSHOT
        // simply clone
        case "snapshot":
          try {
            let tmpDetachedClone = JSON.parse(
              driver.getNormalizedParameterValue(
                call.request.parameters,
                "detachedVolumesFromSnapshots"
              )
            );
            if (typeof tmpDetachedClone === "boolean") {
              detachedClone = tmpDetachedClone;
            }
          } catch (e) {}

          volumeProperties[VOLUME_CONTENT_SOURCE_ID_PROPERTY_NAME] =
            volume_content_source.snapshot.snapshot_id;
          volume_content_source_snapshot_id =
            volume_content_source.snapshot.snapshot_id;

          // zfs origin property contains parent info, ie: pool0/k8s/test/PVC-111@clone-test
          if (zb.helpers.isZfsSnapshot(volume_content_source_snapshot_id)) {
            fullSnapshotName =
              datasetParentName + "/" + volume_content_source_snapshot_id;
          } else {
            fullSnapshotName =
              snapshotParentDatasetName +
              "/" +
              volume_content_source_snapshot_id +
              "@" +
              VOLUME_SOURCE_CLONE_SNAPSHOT_PREFIX +
              volume_id;
          }

          driver.ctx.logger.debug("full snapshot name: %s", fullSnapshotName);

          if (!zb.helpers.isZfsSnapshot(volume_content_source_snapshot_id)) {
            try {
              await httpApiClient.SnapshotCreate(fullSnapshotName);
            } catch (err) {
              if (
                err.toString().includes("dataset does not exist") ||
                err.toString().includes("not found")
              ) {
                throw new GrpcError(
                  grpc.status.NOT_FOUND,
                  `snapshot source_snapshot_id ${volume_content_source_snapshot_id} does not exist`
                );
              }

              throw err;
            }
          }

          if (detachedClone) {
            try {
              response = await httpApiClient.ReplicationRunOnetime({
                direction: "PUSH",
                transport: "LOCAL",
                source_datasets: [
                  zb.helpers.extractDatasetName(fullSnapshotName),
                ],
                target_dataset: datasetName,
                name_regex: `^${zb.helpers.extractSnapshotName(
                  fullSnapshotName
                )}$`,
                recursive: false,
                retention_policy: "NONE",
                readonly: "IGNORE",
                properties: false,
                only_from_scratch: true,
              });

              let job_id = response;
              let job;

              // wait for job to finish
              while (
                !job ||
                !["SUCCESS", "ABORTED", "FAILED"].includes(job.state)
              ) {
                job = await httpApiClient.CoreGetJobs({ id: job_id });
                job = job[0];
                await GeneralUtils.sleep(3000);
              }

              job.error = job.error || "";

              switch (job.state) {
                case "SUCCESS":
                  break;
                case "FAILED":
                case "ABORTED":
                default:
                  //[EFAULT] Target dataset 'tank/.../clone-test' already exists.
                  if (!job.error.includes("already exists")) {
                    throw new GrpcError(
                      grpc.status.UNKNOWN,
                      `failed to run replication task (${job.state}): ${job.error}`
                    );
                  }
                  break;
              }

              response = await httpApiClient.DatasetSet(
                datasetName,
                volumeProperties
              );
            } catch (err) {
              if (
                err.toString().includes("destination") &&
                err.toString().includes("exists")
              ) {
                // move along
              } else {
                throw err;
              }
            }

            // remove snapshots from target
            await this.removeSnapshotsFromDatatset(datasetName);
          } else {
            try {
              response = await httpApiClient.CloneCreate(
                fullSnapshotName,
                datasetName,
                {
                  dataset_properties: volumeProperties,
                }
              );
            } catch (err) {
              if (
                err.toString().includes("dataset does not exist") ||
                err.toString().includes("not found")
              ) {
                throw new GrpcError(
                  grpc.status.NOT_FOUND,
                  "dataset does not exists"
                );
              }

              throw err;
            }
          }

          if (!zb.helpers.isZfsSnapshot(volume_content_source_snapshot_id)) {
            try {
              // schedule snapshot removal from source
              await httpApiClient.SnapshotDelete(fullSnapshotName, {
                defer: true,
              });
            } catch (err) {
              if (
                err.toString().includes("dataset does not exist") ||
                err.toString().includes("not found")
              ) {
                throw new GrpcError(
                  grpc.status.NOT_FOUND,
                  `snapshot source_snapshot_id ${volume_content_source_snapshot_id} does not exist`
                );
              }

              throw err;
            }
          }

          break;
        // must be available when adverstising CLONE_VOLUME
        // create snapshot first, then clone
        case "volume":
          try {
            let tmpDetachedClone = JSON.parse(
              driver.getNormalizedParameterValue(
                call.request.parameters,
                "detachedVolumesFromVolumes"
              )
            );
            if (typeof tmpDetachedClone === "boolean") {
              detachedClone = tmpDetachedClone;
            }
          } catch (e) {}

          volumeProperties[VOLUME_CONTENT_SOURCE_ID_PROPERTY_NAME] =
            volume_content_source.volume.volume_id;
          volume_content_source_volume_id =
            volume_content_source.volume.volume_id;

          fullSnapshotName =
            datasetParentName +
            "/" +
            volume_content_source_volume_id +
            "@" +
            VOLUME_SOURCE_CLONE_SNAPSHOT_PREFIX +
            volume_id;

          driver.ctx.logger.debug("full snapshot name: %s", fullSnapshotName);

          // create snapshot
          try {
            response = await httpApiClient.SnapshotCreate(fullSnapshotName);
          } catch (err) {
            if (
              err.toString().includes("dataset does not exist") ||
              err.toString().includes("not found")
            ) {
              throw new GrpcError(
                grpc.status.NOT_FOUND,
                "dataset does not exists"
              );
            }

            throw err;
          }

          if (detachedClone) {
            try {
              response = await httpApiClient.ReplicationRunOnetime({
                direction: "PUSH",
                transport: "LOCAL",
                source_datasets: [
                  zb.helpers.extractDatasetName(fullSnapshotName),
                ],
                target_dataset: datasetName,
                name_regex: `^${zb.helpers.extractSnapshotName(
                  fullSnapshotName
                )}$`,
                recursive: false,
                retention_policy: "NONE",
                readonly: "IGNORE",
                properties: false,
                only_from_scratch: true,
              });

              let job_id = response;
              let job;

              // wait for job to finish
              while (
                !job ||
                !["SUCCESS", "ABORTED", "FAILED"].includes(job.state)
              ) {
                job = await httpApiClient.CoreGetJobs({ id: job_id });
                job = job[0];
                await GeneralUtils.sleep(3000);
              }

              job.error = job.error || "";

              switch (job.state) {
                case "SUCCESS":
                  break;
                case "FAILED":
                case "ABORTED":
                default:
                  //[EFAULT] Target dataset 'tank/.../clone-test' already exists.
                  if (!job.error.includes("already exists")) {
                    throw new GrpcError(
                      grpc.status.UNKNOWN,
                      `failed to run replication task (${job.state}): ${job.error}`
                    );
                  }
                  break;
              }
            } catch (err) {
              if (
                err.toString().includes("destination") &&
                err.toString().includes("exists")
              ) {
                // move along
              } else {
                throw err;
              }
            }

            response = await httpApiClient.DatasetSet(
              datasetName,
              volumeProperties
            );

            // remove snapshots from target
            await this.removeSnapshotsFromDatatset(datasetName);

            // remove snapshot from source
            await httpApiClient.SnapshotDelete(fullSnapshotName, {
              defer: true,
            });
          } else {
            // create clone
            // zfs origin property contains parent info, ie: pool0/k8s/test/PVC-111@clone-test
            try {
              response = await httpApiClient.CloneCreate(
                fullSnapshotName,
                datasetName,
                {
                  dataset_properties: volumeProperties,
                }
              );
            } catch (err) {
              if (
                err.toString().includes("dataset does not exist") ||
                err.toString().includes("not found")
              ) {
                throw new GrpcError(
                  grpc.status.NOT_FOUND,
                  "dataset does not exists"
                );
              }

              throw err;
            }
          }
          break;
        default:
          throw new GrpcError(
            grpc.status.INVALID_ARGUMENT,
            `invalid volume_content_source type: ${volume_content_source.type}`
          );
          break;
      }
    } else {
      // force blocksize on newly created zvols
      if (driverZfsResourceType == "volume") {
        volumeProperties.volblocksize = zvolBlocksize;
      }

      await httpApiClient.DatasetCreate(datasetName, {
        ...httpApiClient.getSystemProperties(volumeProperties),
        type: driverZfsResourceType.toUpperCase(),
        volsize: driverZfsResourceType == "volume" ? capacity_bytes : undefined,
        sparse: driverZfsResourceType == "volume" ? sparse : undefined,
        create_ancestors: true,
        share_type: driver.getDriverShareType().includes("smb")
          ? "SMB"
          : "GENERIC",
        user_properties: httpApiClient.getPropertiesKeyValueArray(
          httpApiClient.getUserProperties(volumeProperties)
        ),
      });
    }

    let setProps = false;
    let setPerms = false;
    let properties = {};
    let volume_context = {};

    switch (driverZfsResourceType) {
      case "filesystem":
        // set quota
        if (this.options.zfs.datasetEnableQuotas) {
          setProps = true;
          properties.refquota = capacity_bytes;
        }

        // set reserve
        if (this.options.zfs.datasetEnableReservation) {
          setProps = true;
          properties.refreservation = capacity_bytes;
        }

        // quota for dataset and all children
        // reserved for dataset and all children

        // dedup
        // ro?
        // record size

        // set properties
        if (setProps) {
          await httpApiClient.DatasetSet(datasetName, properties);
        }

        // get properties needed for remaining calls
        properties = await httpApiClient.DatasetGet(datasetName, [
          "mountpoint",
          "refquota",
          "compression",
          VOLUME_CSI_NAME_PROPERTY_NAME,
          VOLUME_CONTENT_SOURCE_TYPE_PROPERTY_NAME,
          VOLUME_CONTENT_SOURCE_ID_PROPERTY_NAME,
        ]);
        driver.ctx.logger.debug("zfs props data: %j", properties);

        // set mode
        let perms = {
          path: properties.mountpoint.value,
        };
        if (this.options.zfs.datasetPermissionsMode) {
          setPerms = true;
          perms.mode = this.options.zfs.datasetPermissionsMode;
        }

        // set ownership
        if (
          this.options.zfs.hasOwnProperty("datasetPermissionsUser") ||
          this.options.zfs.hasOwnProperty("datasetPermissionsGroup")
        ) {
          setPerms = true;
        }

        // user
        if (this.options.zfs.hasOwnProperty("datasetPermissionsUser")) {
          if (
            String(this.options.zfs.datasetPermissionsUser).match(/^[0-9]+$/) ==
            null
          ) {
            throw new GrpcError(
              grpc.status.FAILED_PRECONDITION,
              `datasetPermissionsUser must be numeric: ${this.options.zfs.datasetPermissionsUser}`
            );
          }
          perms.uid = Number(this.options.zfs.datasetPermissionsUser);
        }

        // group
        if (this.options.zfs.hasOwnProperty("datasetPermissionsGroup")) {
          if (
            String(this.options.zfs.datasetPermissionsGroup).match(
              /^[0-9]+$/
            ) == null
          ) {
            throw new GrpcError(
              grpc.status.FAILED_PRECONDITION,
              `datasetPermissionsGroup must be numeric: ${this.options.zfs.datasetPermissionsGroup}`
            );
          }
          perms.gid = Number(this.options.zfs.datasetPermissionsGroup);
        }

        if (setPerms) {
          response = await httpApiClient.FilesystemSetperm(perms);
          await httpApiClient.CoreWaitForJob(response, 30);
          // SetPerm does not alter ownership with extended ACLs
          // run this in addition just for good measure
          if (perms.uid || perms.gid) {
            response = await httpApiClient.FilesystemChown({
              path: perms.path,
              uid: perms.uid,
              gid: perms.gid,
            });
            await httpApiClient.CoreWaitForJob(response, 30);
          }
        }

        // set acls
        // TODO: this is unsfafe approach, make it better
        // probably could see if ^-.*\s and split and then shell escape
        if (this.options.zfs.datasetPermissionsAcls) {
          for (const acl of this.options.zfs.datasetPermissionsAcls) {
            perms = {
              path: properties.mountpoint.value,
              dacl: acl,
            };
            // TODO: FilesystemSetacl?
          }
        }

        break;
      case "volume":
        // set properties
        // set reserve
        setProps = true;

        // this should be already set, but when coming from a volume source
        // it may not match that of the source
        properties.volsize = capacity_bytes;

        // dedup
        // on, off, verify
        // zfs set dedup=on tank/home
        // restore default must use the below
        // zfs inherit [-rS] property filesystem|volume|snapshot…
        if (
          (typeof this.options.zfs.zvolDedup === "string" ||
            this.options.zfs.zvolDedup instanceof String) &&
          this.options.zfs.zvolDedup.length > 0
        ) {
          properties.dedup = this.options.zfs.zvolDedup;
        }

        // compression
        // lz4, gzip-9, etc
        if (
          (typeof this.options.zfs.zvolCompression === "string" ||
            this.options.zfs.zvolCompression instanceof String) &&
          this.options.zfs.zvolCompression > 0
        ) {
          properties.compression = this.options.zfs.zvolCompression;
        }

        if (setProps) {
          await httpApiClient.DatasetSet(datasetName, properties);
        }

        break;
    }

    volume_context = await this.createShare(call, datasetName);
    await httpApiClient.DatasetSet(datasetName, {
      [SHARE_VOLUME_CONTEXT_PROPERTY_NAME]: JSON.stringify(volume_context),
    });

    volume_context["provisioner_driver"] = driver.options.driver;
    if (driver.options.instance_id) {
      volume_context["provisioner_driver_instance_id"] =
        driver.options.instance_id;
    }

    // set this just before sending out response so we know if volume completed
    // this should give us a relatively sane way to clean up artifacts over time
    await httpApiClient.DatasetSet(datasetName, {
      [SUCCESS_PROPERTY_NAME]: "true",
    });

    const res = {
      volume: {
        volume_id,
        //capacity_bytes: capacity_bytes, // kubernetes currently pukes if capacity is returned as 0
        capacity_bytes:
          this.options.zfs.datasetEnableQuotas ||
          driverZfsResourceType == "volume"
            ? capacity_bytes
            : 0,
        content_source: volume_content_source,
        volume_context,
      },
    };

    return res;
  }

  /**
   * Delete a volume
   *
   * Deleting a volume consists of the following steps:
   * 1. delete the nfs share
   * 2. delete the dataset
   *
   * @param {*} call
   */
  async DeleteVolume(call) {
    const driver = this;
    const httpApiClient = await this.getTrueNASHttpApiClient();
    const zb = await this.getZetabyte();

    let datasetParentName = this.getVolumeParentDatasetName();
    let name = call.request.volume_id;

    if (!datasetParentName) {
      throw new GrpcError(
        grpc.status.FAILED_PRECONDITION,
        `invalid configuration: missing datasetParentName`
      );
    }

    if (!name) {
      throw new GrpcError(
        grpc.status.INVALID_ARGUMENT,
        `volume_id is required`
      );
    }

    const datasetName = datasetParentName + "/" + name;
    let properties;

    // get properties needed for remaining calls
    try {
      properties = await httpApiClient.DatasetGet(datasetName, [
        "mountpoint",
        "origin",
        "refquota",
        "compression",
        VOLUME_CSI_NAME_PROPERTY_NAME,
      ]);
    } catch (err) {
      let ignore = false;
      if (err.toString().includes("dataset does not exist")) {
        ignore = true;
      }

      if (!ignore) {
        throw err;
      }
    }

    driver.ctx.logger.debug("dataset properties: %j", properties);

    // deleteStrategy
    const delete_strategy = _.get(
      driver.options,
      "_private.csi.volume.deleteStrategy",
      ""
    );

    if (delete_strategy == "retain") {
      return {};
    }

    // remove share resources
    await this.deleteShare(call, datasetName);

    // remove parent snapshot if appropriate with defer
    if (
      properties &&
      properties.origin &&
      properties.origin.value != "-" &&
      zb.helpers
        .extractSnapshotName(properties.origin.value)
        .startsWith(VOLUME_SOURCE_CLONE_SNAPSHOT_PREFIX)
    ) {
      driver.ctx.logger.debug(
        "removing with defer source snapshot: %s",
        properties.origin.value
      );

      try {
        await httpApiClient.SnapshotDelete(properties.origin.value, {
          defer: true,
        });
      } catch (err) {
        if (err.toString().includes("snapshot has dependent clones")) {
          throw new GrpcError(
            grpc.status.FAILED_PRECONDITION,
            "snapshot has dependent clones"
          );
        }
        throw err;
      }
    }

    // NOTE: -f does NOT allow deletes if dependent filesets exist
    // NOTE: -R will recursively delete items + dependent filesets
    // delete dataset
    try {
      await GeneralUtils.retry(
        12,
        5000,
        async () => {
          await httpApiClient.DatasetDelete(datasetName, {
            recursive: true,
            force: true,
          });
        },
        {
          retryCondition: (err) => {
            if (
              err.toString().includes("dataset is busy") ||
              err.toString().includes("target is busy")
            ) {
              return true;
            }
            return false;
          },
        }
      );
    } catch (err) {
      if (err.toString().includes("filesystem has dependent clones")) {
        throw new GrpcError(
          grpc.status.FAILED_PRECONDITION,
          "filesystem has dependent clones"
        );
      }

      throw err;
    }

    return {};
  }

  /**
   *
   * @param {*} call
   */
  async ControllerExpandVolume(call) {
    const driver = this;
    const driverZfsResourceType = this.getDriverZfsResourceType();
    const httpApiClient = await this.getTrueNASHttpApiClient();
    const zb = await this.getZetabyte();

    let datasetParentName = this.getVolumeParentDatasetName();
    let name = call.request.volume_id;

    if (!datasetParentName) {
      throw new GrpcError(
        grpc.status.FAILED_PRECONDITION,
        `invalid configuration: missing datasetParentName`
      );
    }

    if (!name) {
      throw new GrpcError(
        grpc.status.INVALID_ARGUMENT,
        `volume_id is required`
      );
    }

    const datasetName = datasetParentName + "/" + name;

    let capacity_bytes =
      call.request.capacity_range.required_bytes ||
      call.request.capacity_range.limit_bytes;
    if (!capacity_bytes) {
      //should never happen, value must be set
      throw new GrpcError(
        grpc.status.INVALID_ARGUMENT,
        `volume capacity is required (either required_bytes or limit_bytes)`
      );
    }

    if (capacity_bytes && driverZfsResourceType == "volume") {
      //make sure to align capacity_bytes with zvol blocksize
      //volume size must be a multiple of volume block size
      let properties = await httpApiClient.DatasetGet(datasetName, [
        "volblocksize",
      ]);
      capacity_bytes = zb.helpers.generateZvolSize(
        capacity_bytes,
        properties.volblocksize.rawvalue
      );
    }

    if (
      call.request.capacity_range.required_bytes > 0 &&
      call.request.capacity_range.limit_bytes > 0 &&
      call.request.capacity_range.required_bytes >
        call.request.capacity_range.limit_bytes
    ) {
      throw new GrpcError(
        grpc.status.INVALID_ARGUMENT,
        `required_bytes is greather than limit_bytes`
      );
    }

    // ensure *actual* capacity is not greater than limit
    if (
      call.request.capacity_range.limit_bytes &&
      call.request.capacity_range.limit_bytes > 0 &&
      capacity_bytes > call.request.capacity_range.limit_bytes
    ) {
      throw new GrpcError(
        grpc.status.OUT_OF_RANGE,
        `required volume capacity is greater than limit`
      );
    }

    let setProps = false;
    let properties = {};

    switch (driverZfsResourceType) {
      case "filesystem":
        // set quota
        if (this.options.zfs.datasetEnableQuotas) {
          setProps = true;
          properties.refquota = capacity_bytes;
        }

        // set reserve
        if (this.options.zfs.datasetEnableReservation) {
          setProps = true;
          properties.refreservation = capacity_bytes;
        }
        break;
      case "volume":
        properties.volsize = capacity_bytes;
        setProps = true;

        // managed automatically for zvols
        //if (this.options.zfs.zvolEnableReservation) {
        //  properties.refreservation = capacity_bytes;
        //}
        break;
    }

    if (setProps) {
      await httpApiClient.DatasetSet(datasetName, properties);
    }

    await this.expandVolume(call, datasetName);

    return {
      capacity_bytes:
        this.options.zfs.datasetEnableQuotas ||
        driverZfsResourceType == "volume"
          ? capacity_bytes
          : 0,
      node_expansion_required: driverZfsResourceType == "volume" ? true : false,
    };
  }

  /**
   * TODO: consider volume_capabilities?
   *
   * @param {*} call
   */
  async GetCapacity(call) {
    const driver = this;
    const httpApiClient = await this.getTrueNASHttpApiClient();
    const zb = await this.getZetabyte();

    let datasetParentName = this.getVolumeParentDatasetName();

    if (!datasetParentName) {
      throw new GrpcError(
        grpc.status.FAILED_PRECONDITION,
        `invalid configuration: missing datasetParentName`
      );
    }

    if (call.request.volume_capabilities) {
      const result = this.assertCapabilities(call.request.volume_capabilities);

      if (result.valid !== true) {
        return { available_capacity: 0 };
      }
    }

    const datasetName = datasetParentName;

    await httpApiClient.DatasetCreate(datasetName, {
      create_ancestors: true,
    });

    let properties;
    properties = await httpApiClient.DatasetGet(datasetName, ["available"]);
    let minimum_volume_size = await driver.getMinimumVolumeSize();

    return {
      available_capacity: Number(properties.available.rawvalue),
      minimum_volume_size:
        minimum_volume_size > 0
          ? {
              value: Number(minimum_volume_size),
            }
          : undefined,
    };
  }

  /**
   * Get a single volume
   *
   * @param {*} call
   */
  async ControllerGetVolume(call) {
    const driver = this;
    const driverZfsResourceType = this.getDriverZfsResourceType();
    const httpApiClient = await this.getTrueNASHttpApiClient();
    const zb = await this.getZetabyte();

    let datasetParentName = this.getVolumeParentDatasetName();
    let response;
    let name = call.request.volume_id;

    if (!datasetParentName) {
      throw new GrpcError(
        grpc.status.FAILED_PRECONDITION,
        `invalid configuration: missing datasetParentName`
      );
    }

    if (!name) {
      throw new GrpcError(
        grpc.status.INVALID_ARGUMENT,
        `volume_id is required`
      );
    }

    const datasetName = datasetParentName + "/" + name;

    try {
      response = await httpApiClient.DatasetGet(datasetName, [
        "name",
        "mountpoint",
        "refquota",
        "available",
        "used",
        VOLUME_CSI_NAME_PROPERTY_NAME,
        VOLUME_CONTENT_SOURCE_TYPE_PROPERTY_NAME,
        VOLUME_CONTENT_SOURCE_ID_PROPERTY_NAME,
        "volsize",
        MANAGED_PROPERTY_NAME,
        SHARE_VOLUME_CONTEXT_PROPERTY_NAME,
        SUCCESS_PROPERTY_NAME,
        VOLUME_CONTEXT_PROVISIONER_INSTANCE_ID_PROPERTY_NAME,
        VOLUME_CONTEXT_PROVISIONER_DRIVER_PROPERTY_NAME,
      ]);
    } catch (err) {
      if (err.toString().includes("dataset does not exist")) {
        throw new GrpcError(grpc.status.NOT_FOUND, `volume_id is missing`);
      }

      throw err;
    }

    let row = {};
    for (let p in response) {
      row[p] = response[p].rawvalue;
    }

    driver.ctx.logger.debug("list volumes result: %j", row);
    let volume = await driver.populateCsiVolumeFromData(row);
    let status = await driver.getVolumeStatus(datasetName);

    let res = { volume };
    if (status) {
      res.status = status;
    }

    return res;
  }

  /**
   *
   * TODO: check capability to ensure not asking about block volumes
   *
   * @param {*} call
   */
  async ListVolumes(call) {
    const driver = this;
    const driverZfsResourceType = this.getDriverZfsResourceType();
    const httpClient = await this.getHttpClient();
    const httpApiClient = await this.getTrueNASHttpApiClient();
    const zb = await this.getZetabyte();

    let datasetParentName = this.getVolumeParentDatasetName();
    let entries = [];
    let entries_length = 0;
    let next_token;
    let uuid;
    let response;
    let endpoint;

    const max_entries = call.request.max_entries;
    const starting_token = call.request.starting_token;

    // get data from cache and return immediately
    if (starting_token) {
      let parts = starting_token.split(":");
      uuid = parts[0];
      let start_position = parseInt(parts[1]);
      let end_position;
      if (max_entries > 0) {
        end_position = start_position + max_entries;
      }
      entries = this.ctx.cache.get(`ListVolumes:result:${uuid}`);
      if (entries) {
        entries_length = entries.length;
        entries = entries.slice(start_position, end_position);
        if (max_entries > 0 && end_position > entries_length) {
          next_token = `${uuid}:${end_position}`;
        } else {
          next_token = null;
        }
        const data = {
          entries: entries,
          next_token: next_token,
        };

        return data;
      } else {
        throw new GrpcError(
          grpc.status.ABORTED,
          `invalid starting_token: ${starting_token}`
        );
      }
    }

    if (!datasetParentName) {
      throw new GrpcError(
        grpc.status.FAILED_PRECONDITION,
        `invalid configuration: missing datasetParentName`
      );
    }

    const datasetName = datasetParentName;
    const rows = [];

    endpoint = `/pool/dataset/id/${encodeURIComponent(datasetName)}`;
    response = await httpClient.get(endpoint);

    //console.log(response);

    if (response.statusCode == 404) {
      return {
        entries: [],
        next_token: null,
      };
    }
    if (response.statusCode == 200) {
      for (let child of response.body.children) {
        let child_properties = httpApiClient.normalizeProperties(child, [
          "name",
          "mountpoint",
          "refquota",
          "available",
          "used",
          VOLUME_CSI_NAME_PROPERTY_NAME,
          VOLUME_CONTENT_SOURCE_TYPE_PROPERTY_NAME,
          VOLUME_CONTENT_SOURCE_ID_PROPERTY_NAME,
          "volsize",
          MANAGED_PROPERTY_NAME,
          SHARE_VOLUME_CONTEXT_PROPERTY_NAME,
          SUCCESS_PROPERTY_NAME,
          VOLUME_CONTEXT_PROVISIONER_INSTANCE_ID_PROPERTY_NAME,
          VOLUME_CONTEXT_PROVISIONER_DRIVER_PROPERTY_NAME,
        ]);

        let row = {};
        for (let p in child_properties) {
          row[p] = child_properties[p].rawvalue;
        }

        rows.push(row);
      }
    }

    driver.ctx.logger.debug("list volumes result: %j", rows);

    entries = [];
    for (let row of rows) {
      // ignore rows were csi_name is empty
      if (row[MANAGED_PROPERTY_NAME] != "true") {
        continue;
      }

      let volume_id = row["name"].replace(
        new RegExp("^" + datasetName + "/"),
        ""
      );

      let volume = await driver.populateCsiVolumeFromData(row);
      if (volume) {
        let status = await driver.getVolumeStatus(volume_id);
        entries.push({
          volume,
          status,
        });
      }
    }

    if (max_entries && entries.length > max_entries) {
      uuid = uuidv4();
      this.ctx.cache.set(`ListVolumes:result:${uuid}`, entries);
      next_token = `${uuid}:${max_entries}`;
      entries = entries.slice(0, max_entries);
    }

    const data = {
      entries: entries,
      next_token: next_token,
    };

    return data;
  }

  /**
   *
   * @param {*} call
   */
  async ListSnapshots(call) {
    const driver = this;
    const driverZfsResourceType = this.getDriverZfsResourceType();
    const httpClient = await this.getHttpClient();
    const httpApiClient = await this.getTrueNASHttpApiClient();
    const zb = await this.getZetabyte();

    let entries = [];
    let entries_length = 0;
    let next_token;
    let uuid;

    const max_entries = call.request.max_entries;
    const starting_token = call.request.starting_token;

    let types = [];

    const volumeParentDatasetName = this.getVolumeParentDatasetName();
    const snapshotParentDatasetName =
      this.getDetachedSnapshotParentDatasetName();

    // get data from cache and return immediately
    if (starting_token) {
      let parts = starting_token.split(":");
      uuid = parts[0];
      let start_position = parseInt(parts[1]);
      let end_position;
      if (max_entries > 0) {
        end_position = start_position + max_entries;
      }
      entries = this.ctx.cache.get(`ListSnapshots:result:${uuid}`);
      if (entries) {
        entries_length = entries.length;
        entries = entries.slice(start_position, end_position);
        if (max_entries > 0 && end_position > entries_length) {
          next_token = `${uuid}:${end_position}`;
        } else {
          next_token = null;
        }
        const data = {
          entries: entries,
          next_token: next_token,
        };

        return data;
      } else {
        throw new GrpcError(
          grpc.status.ABORTED,
          `invalid starting_token: ${starting_token}`
        );
      }
    }

    if (!volumeParentDatasetName) {
      // throw error
      throw new GrpcError(
        grpc.status.FAILED_PRECONDITION,
        `invalid configuration: missing datasetParentName`
      );
    }

    let snapshot_id = call.request.snapshot_id;
    let source_volume_id = call.request.source_volume_id;

    entries = [];
    for (let loopType of ["snapshot", "filesystem"]) {
      let endpoint, response, operativeFilesystem, operativeFilesystemType;
      let datasetParentName;
      switch (loopType) {
        case "snapshot":
          datasetParentName = volumeParentDatasetName;
          types = ["snapshot"];
          // should only send 1 of snapshot_id or source_volume_id, preferring the former if sent
          if (snapshot_id) {
            if (!zb.helpers.isZfsSnapshot(snapshot_id)) {
              continue;
            }
            operativeFilesystem = volumeParentDatasetName + "/" + snapshot_id;
            operativeFilesystemType = 3;
          } else if (source_volume_id) {
            operativeFilesystem =
              volumeParentDatasetName + "/" + source_volume_id;
            operativeFilesystemType = 2;
          } else {
            operativeFilesystem = volumeParentDatasetName;
            operativeFilesystemType = 1;
          }
          break;
        case "filesystem":
          datasetParentName = snapshotParentDatasetName;
          if (!datasetParentName) {
            continue;
          }
          if (driverZfsResourceType == "filesystem") {
            types = ["filesystem"];
          } else {
            types = ["volume"];
          }

          // should only send 1 of snapshot_id or source_volume_id, preferring the former if sent
          if (snapshot_id) {
            if (zb.helpers.isZfsSnapshot(snapshot_id)) {
              continue;
            }
            operativeFilesystem = snapshotParentDatasetName + "/" + snapshot_id;
            operativeFilesystemType = 3;
          } else if (source_volume_id) {
            operativeFilesystem =
              snapshotParentDatasetName + "/" + source_volume_id;
            operativeFilesystemType = 2;
          } else {
            operativeFilesystem = snapshotParentDatasetName;
            operativeFilesystemType = 1;
          }
          break;
      }

      let rows = [];

      try {
        let zfsProperties = [
          "name",
          "creation",
          "mountpoint",
          "refquota",
          "available",
          "used",
          "volsize",
          "referenced",
          "logicalreferenced",
          VOLUME_CSI_NAME_PROPERTY_NAME,
          SNAPSHOT_CSI_NAME_PROPERTY_NAME,
          MANAGED_PROPERTY_NAME,
        ];
        /*
        response = await zb.zfs.list(
          operativeFilesystem,
          ,
          { types, recurse: true }
        );
        */

        //console.log(types, operativeFilesystem, operativeFilesystemType);

        if (types.includes("snapshot")) {
          switch (operativeFilesystemType) {
            case 3:
              // get explicit snapshot
              response = await httpApiClient.SnapshotGet(
                operativeFilesystem,
                zfsProperties
              );

              let row = {};
              for (let p in response) {
                row[p] = response[p].rawvalue;
              }
              rows.push(row);
              break;
            case 2:
              // get snapshots connected to the to source_volume_id
              endpoint = `/pool/dataset/id/${encodeURIComponent(
                operativeFilesystem
              )}`;
              response = await httpClient.get(endpoint, {
                "extra.snapshots": 1,
                "extra.snapshots_properties": JSON.stringify(zfsProperties),
              });
              if (response.statusCode == 404) {
                throw new Error("dataset does not exist");
              } else if (response.statusCode == 200) {
                for (let snapshot of response.body.snapshots) {
                  let row = {};
                  for (let p in snapshot.properties) {
                    row[p] = snapshot.properties[p].rawvalue;
                  }
                  rows.push(row);
                }
              } else {
                throw new Error(`unhandled statusCode: ${response.statusCode}`);
              }
              break;
            case 1:
              // get all snapshot recursively from the parent dataset
              endpoint = `/pool/dataset/id/${encodeURIComponent(
                operativeFilesystem
              )}`;
              response = await httpClient.get(endpoint, {
                "extra.snapshots": 1,
                "extra.snapshots_properties": JSON.stringify(zfsProperties),
              });
              if (response.statusCode == 404) {
                throw new Error("dataset does not exist");
              } else if (response.statusCode == 200) {
                for (let child of response.body.children) {
                  for (let snapshot of child.snapshots) {
                    let row = {};
                    for (let p in snapshot.properties) {
                      row[p] = snapshot.properties[p].rawvalue;
                    }
                    rows.push(row);
                  }
                }
              } else {
                throw new Error(`unhandled statusCode: ${response.statusCode}`);
              }
              break;
            default:
              throw new GrpcError(
                grpc.status.FAILED_PRECONDITION,
                `invalid operativeFilesystemType [${operativeFilesystemType}]`
              );
              break;
          }
        } else if (types.includes("filesystem") || types.includes("volume")) {
          switch (operativeFilesystemType) {
            case 3:
              // get explicit snapshot
              response = await httpApiClient.DatasetGet(
                operativeFilesystem,
                zfsProperties
              );

              let row = {};
              for (let p in response) {
                row[p] = response[p].rawvalue;
              }
              rows.push(row);
              break;
            case 2:
              // get snapshots connected to the to source_volume_id
              endpoint = `/pool/dataset/id/${encodeURIComponent(
                operativeFilesystem
              )}`;
              response = await httpClient.get(endpoint);
              if (response.statusCode == 404) {
                throw new Error("dataset does not exist");
              } else if (response.statusCode == 200) {
                for (let child of response.body.children) {
                  let i_response = httpApiClient.normalizeProperties(
                    child,
                    zfsProperties
                  );
                  let row = {};
                  for (let p in i_response) {
                    row[p] = i_response[p].rawvalue;
                  }
                  rows.push(row);
                }
              } else {
                throw new Error(`unhandled statusCode: ${response.statusCode}`);
              }
              break;
            case 1:
              // get all snapshot recursively from the parent dataset
              endpoint = `/pool/dataset/id/${encodeURIComponent(
                operativeFilesystem
              )}`;
              response = await httpClient.get(endpoint);
              if (response.statusCode == 404) {
                throw new Error("dataset does not exist");
              } else if (response.statusCode == 200) {
                for (let child of response.body.children) {
                  for (let grandchild of child.children) {
                    let i_response = httpApiClient.normalizeProperties(
                      grandchild,
                      zfsProperties
                    );
                    let row = {};
                    for (let p in i_response) {
                      row[p] = i_response[p].rawvalue;
                    }
                    rows.push(row);
                  }
                }
              } else {
                throw new Error(`unhandled statusCode: ${response.statusCode}`);
              }
              break;
            default:
              throw new GrpcError(
                grpc.status.FAILED_PRECONDITION,
                `invalid operativeFilesystemType [${operativeFilesystemType}]`
              );
              break;
          }
        } else {
          throw new GrpcError(
            grpc.status.FAILED_PRECONDITION,
            `invalid zfs types [${types.join(",")}]`
          );
        }
      } catch (err) {
        let message;
        if (err.toString().includes("dataset does not exist")) {
          switch (operativeFilesystemType) {
            case 1:
              //message = `invalid configuration: datasetParentName ${datasetParentName} does not exist`;
              continue;
              break;
            case 2:
              message = `source_volume_id ${source_volume_id} does not exist`;
              continue;
              break;
            case 3:
              message = `snapshot_id ${snapshot_id} does not exist`;
              continue;
              break;
          }
          throw new GrpcError(grpc.status.NOT_FOUND, message);
        }
        throw new GrpcError(grpc.status.FAILED_PRECONDITION, err.toString());
      }

      rows.forEach((row) => {
        // skip any snapshots not explicitly created by CO
        if (row[MANAGED_PROPERTY_NAME] != "true") {
          return;
        }

        // ignore snapshots that are not explicit CO snapshots
        if (
          !zb.helpers.isPropertyValueSet(row[SNAPSHOT_CSI_NAME_PROPERTY_NAME])
        ) {
          return;
        }

        // strip parent dataset
        let source_volume_id = row["name"].replace(
          new RegExp("^" + datasetParentName + "/"),
          ""
        );

        // strip snapshot details (@snapshot-name)
        if (source_volume_id.includes("@")) {
          source_volume_id = source_volume_id.substring(
            0,
            source_volume_id.indexOf("@")
          );
        } else {
          source_volume_id = source_volume_id.replace(
            new RegExp("/" + row[SNAPSHOT_CSI_NAME_PROPERTY_NAME] + "$"),
            ""
          );
        }

        if (source_volume_id == datasetParentName) {
          return;
        }

        // TODO: properly handle use-case where datasetEnableQuotas is not turned on
        let size_bytes = 0;
        if (driverZfsResourceType == "filesystem") {
          // independent of detached snapshots when creating a volume from a 'snapshot'
          // we could be using detached clones (ie: send/receive)
          // so we must be cognizant and use the highest possible value here
          // note that whatever value is returned here can/will essentially impact the refquota
          // value of a derived volume
          size_bytes = GeneralUtils.getLargestNumber(
            row.referenced,
            row.logicalreferenced
          );
        } else {
          // get the size of the parent volume
          size_bytes = row.volsize;
        }

        if (source_volume_id)
          entries.push({
            snapshot: {
              /**
               * The purpose of this field is to give CO guidance on how much space
               * is needed to create a volume from this snapshot.
               *
               * In that vein, I think it's best to return 0 here given the
               * unknowns of 'cow' implications.
               */
              size_bytes,

              // remove parent dataset details
              snapshot_id: row["name"].replace(
                new RegExp("^" + datasetParentName + "/"),
                ""
              ),
              source_volume_id: source_volume_id,
              //https://github.com/protocolbuffers/protobuf/blob/master/src/google/protobuf/timestamp.proto
              creation_time: {
                seconds: zb.helpers.isPropertyValueSet(row["creation"])
                  ? row["creation"]
                  : 0,
                nanos: 0,
              },
              ready_to_use: true,
            },
          });
      });
    }

    if (max_entries && entries.length > max_entries) {
      uuid = uuidv4();
      this.ctx.cache.set(`ListSnapshots:result:${uuid}`, entries);
      next_token = `${uuid}:${max_entries}`;
      entries = entries.slice(0, max_entries);
    }

    const data = {
      entries: entries,
      next_token: next_token,
    };

    return data;
  }

  /**
   *
   * @param {*} call
   */
  async CreateSnapshot(call) {
    const driver = this;
    const driverZfsResourceType = this.getDriverZfsResourceType();
    const httpClient = await this.getHttpClient();
    const httpApiClient = await this.getTrueNASHttpApiClient();
    const zb = await this.getZetabyte();

    let size_bytes = 0;
    let detachedSnapshot = false;
    try {
      let tmpDetachedSnapshot = JSON.parse(
        driver.getNormalizedParameterValue(
          call.request.parameters,
          "detachedSnapshots"
        )
      ); // snapshot class parameter
      if (typeof tmpDetachedSnapshot === "boolean") {
        detachedSnapshot = tmpDetachedSnapshot;
      }
    } catch (e) {}

    let response;
    const volumeParentDatasetName = this.getVolumeParentDatasetName();
    let datasetParentName;
    let snapshotProperties = {};
    let types = [];

    if (detachedSnapshot) {
      datasetParentName = this.getDetachedSnapshotParentDatasetName();
      if (driverZfsResourceType == "filesystem") {
        types.push("filesystem");
      } else {
        types.push("volume");
      }
    } else {
      datasetParentName = this.getVolumeParentDatasetName();
      types.push("snapshot");
    }

    if (!datasetParentName) {
      throw new GrpcError(
        grpc.status.FAILED_PRECONDITION,
        `invalid configuration: missing datasetParentName`
      );
    }

    // both these are required
    let source_volume_id = call.request.source_volume_id;
    let name = call.request.name;

    if (!source_volume_id) {
      throw new GrpcError(
        grpc.status.INVALID_ARGUMENT,
        `snapshot source_volume_id is required`
      );
    }

    if (!name) {
      throw new GrpcError(
        grpc.status.INVALID_ARGUMENT,
        `snapshot name is required`
      );
    }

    const datasetName = datasetParentName + "/" + source_volume_id;
    snapshotProperties[SNAPSHOT_CSI_NAME_PROPERTY_NAME] = name;
    snapshotProperties[SNAPSHOT_CSI_SOURCE_VOLUME_ID_PROPERTY_NAME] =
      source_volume_id;
    snapshotProperties[MANAGED_PROPERTY_NAME] = "true";

    driver.ctx.logger.verbose("requested snapshot name: %s", name);

    let invalid_chars;
    invalid_chars = name.match(/[^a-z0-9_\-:.+]+/gi);
    if (invalid_chars) {
      invalid_chars = String.prototype.concat(
        ...new Set(invalid_chars.join(""))
      );
      throw new GrpcError(
        grpc.status.INVALID_ARGUMENT,
        `snapshot name contains invalid characters: ${invalid_chars}`
      );
    }

    // https://stackoverflow.com/questions/32106243/regex-to-remove-all-non-alpha-numeric-and-replace-spaces-with/32106277
    name = name.replace(/[^a-z0-9_\-:.+]+/gi, "");

    driver.ctx.logger.verbose("cleansed snapshot name: %s", name);

    // check for other snapshopts with the same name on other volumes and fail as appropriate
    {
      let endpoint;
      let response;

      let datasets = [];
      endpoint = `/pool/dataset/id/${encodeURIComponent(
        this.getDetachedSnapshotParentDatasetName()
      )}`;
      response = await httpClient.get(endpoint);

      switch (response.statusCode) {
        case 200:
          for (let child of response.body.children) {
            datasets = datasets.concat(child.children);
          }
          //console.log(datasets);
          for (let dataset of datasets) {
            let parts = dataset.name.split("/").slice(-2);
            if (parts[1] != name) {
              continue;
            }

            if (parts[0] != source_volume_id) {
              throw new GrpcError(
                grpc.status.ALREADY_EXISTS,
                `snapshot name: ${name} is incompatible with source_volume_id: ${source_volume_id} due to being used with another source_volume_id`
              );
            }
          }
          break;
        case 404:
          break;
        default:
          throw new Error(JSON.stringify(response.body));
      }

      // get all snapshot recursively from the parent dataset
      let snapshots = [];
      endpoint = `/pool/dataset/id/${encodeURIComponent(
        this.getVolumeParentDatasetName()
      )}`;
      response = await httpClient.get(endpoint, {
        "extra.snapshots": 1,
        //"extra.snapshots_properties": JSON.stringify(zfsProperties),
      });

      switch (response.statusCode) {
        case 200:
          for (let child of response.body.children) {
            snapshots = snapshots.concat(child.snapshots);
          }
          //console.log(snapshots);
          for (let snapshot of snapshots) {
            let parts = zb.helpers.extractLeafName(snapshot.name).split("@");
            if (parts[1] != name) {
              continue;
            }

            if (parts[0] != source_volume_id) {
              throw new GrpcError(
                grpc.status.ALREADY_EXISTS,
                `snapshot name: ${name} is incompatible with source_volume_id: ${source_volume_id} due to being used with another source_volume_id`
              );
            }
          }
          break;
        case 404:
          break;
        default:
          throw new Error(JSON.stringify(response.body));
      }
    }

    let fullSnapshotName;
    let snapshotDatasetName;
    let tmpSnapshotName;
    if (detachedSnapshot) {
      fullSnapshotName = datasetName + "/" + name;
    } else {
      fullSnapshotName = datasetName + "@" + name;
    }

    driver.ctx.logger.verbose("full snapshot name: %s", fullSnapshotName);

    if (detachedSnapshot) {
      tmpSnapshotName =
        volumeParentDatasetName +
        "/" +
        source_volume_id +
        "@" +
        VOLUME_SOURCE_DETACHED_SNAPSHOT_PREFIX +
        name;
      snapshotDatasetName = datasetName + "/" + name;

      // create target dataset parent
      await httpApiClient.DatasetCreate(datasetName, {
        create_ancestors: true,
      });

      // create snapshot on source
      try {
        await httpApiClient.SnapshotCreate(tmpSnapshotName);
      } catch (err) {
        if (err.toString().includes("dataset does not exist")) {
          throw new GrpcError(
            grpc.status.FAILED_PRECONDITION,
            `snapshot source_volume_id ${source_volume_id} does not exist`
          );
        }

        throw err;
      }

      try {
        // copy data from source snapshot to target dataset
        response = await httpApiClient.ReplicationRunOnetime({
          direction: "PUSH",
          transport: "LOCAL",
          source_datasets: [zb.helpers.extractDatasetName(tmpSnapshotName)],
          target_dataset: snapshotDatasetName,
          name_regex: `^${zb.helpers.extractSnapshotName(tmpSnapshotName)}$`,
          recursive: false,
          retention_policy: "NONE",
          readonly: "IGNORE",
          properties: false,
          only_from_scratch: true,
        });

        let job_id = response;
        let job;

        // wait for job to finish
        while (!job || !["SUCCESS", "ABORTED", "FAILED"].includes(job.state)) {
          job = await httpApiClient.CoreGetJobs({ id: job_id });
          job = job[0];
          await GeneralUtils.sleep(3000);
        }

        job.error = job.error || "";

        switch (job.state) {
          case "SUCCESS":
            break;
          case "FAILED":
          case "ABORTED":
          default:
            //[EFAULT] Target dataset 'tank/.../clone-test' already exists.
            if (!job.error.includes("already exists")) {
              throw new GrpcError(
                grpc.status.UNKNOWN,
                `failed to run replication task (${job.state}): ${job.error}`
              );
            }
            break;
        }

        //throw new Error("foobar");

        // set properties on target dataset
        response = await httpApiClient.DatasetSet(
          snapshotDatasetName,
          snapshotProperties
        );
      } catch (err) {
        if (
          err.toString().includes("destination") &&
          err.toString().includes("exists")
        ) {
          // move along
        } else {
          throw err;
        }
      }

      // remove snapshot from target
      await httpApiClient.SnapshotDelete(
        snapshotDatasetName +
          "@" +
          zb.helpers.extractSnapshotName(tmpSnapshotName),
        {
          defer: true,
        }
      );

      // remove snapshot from source
      await httpApiClient.SnapshotDelete(tmpSnapshotName, {
        defer: true,
      });
    } else {
      try {
        await httpApiClient.SnapshotCreate(fullSnapshotName, {
          properties: snapshotProperties,
        });
      } catch (err) {
        if (
          err.toString().includes("dataset does not exist") ||
          err.toString().includes("not found")
        ) {
          throw new GrpcError(
            grpc.status.FAILED_PRECONDITION,
            `snapshot source_volume_id ${source_volume_id} does not exist`
          );
        }

        throw err;
      }
    }

    let properties;
    let fetchProperties = [
      "name",
      "creation",
      "mountpoint",
      "refquota",
      "available",
      "used",
      "volsize",
      "referenced",
      "refreservation",
      "logicalused",
      "logicalreferenced",
      VOLUME_CSI_NAME_PROPERTY_NAME,
      SNAPSHOT_CSI_NAME_PROPERTY_NAME,
      SNAPSHOT_CSI_SOURCE_VOLUME_ID_PROPERTY_NAME,
      MANAGED_PROPERTY_NAME,
    ];

    // TODO: let things settle to ensure proper size_bytes is reported
    // sysctl -d vfs.zfs.txg.timeout  # vfs.zfs.txg.timeout: Max seconds worth of delta per txg
    if (detachedSnapshot) {
      properties = await httpApiClient.DatasetGet(
        fullSnapshotName,
        fetchProperties
      );
    } else {
      properties = await httpApiClient.SnapshotGet(
        fullSnapshotName,
        fetchProperties
      );
    }

    driver.ctx.logger.verbose("snapshot properties: %j", properties);

    // TODO: properly handle use-case where datasetEnableQuotas is not turned on
    if (driverZfsResourceType == "filesystem") {
      // independent of detached snapshots when creating a volume from a 'snapshot'
      // we could be using detached clones (ie: send/receive)
      // so we must be cognizant and use the highest possible value here
      // note that whatever value is returned here can/will essentially impact the refquota
      // value of a derived volume
      size_bytes = GeneralUtils.getLargestNumber(
        properties.referenced.rawvalue,
        properties.logicalreferenced.rawvalue
        // TODO: perhaps include minimum volume size here?
      );
    } else {
      // get the size of the parent volume
      size_bytes = properties.volsize.rawvalue;
    }

    // set this just before sending out response so we know if volume completed
    // this should give us a relatively sane way to clean up artifacts over time
    //await zb.zfs.set(fullSnapshotName, { [SUCCESS_PROPERTY_NAME]: "true" });
    if (detachedSnapshot) {
      await httpApiClient.DatasetSet(fullSnapshotName, {
        [SUCCESS_PROPERTY_NAME]: "true",
      });
    } else {
      await httpApiClient.SnapshotSet(fullSnapshotName, {
        [SUCCESS_PROPERTY_NAME]: "true",
      });
    }

    return {
      snapshot: {
        /**
         * The purpose of this field is to give CO guidance on how much space
         * is needed to create a volume from this snapshot.
         *
         * In that vein, I think it's best to return 0 here given the
         * unknowns of 'cow' implications.
         */
        size_bytes,

        // remove parent dataset details
        snapshot_id: properties.name.value.replace(
          new RegExp("^" + datasetParentName + "/"),
          ""
        ),
        source_volume_id: source_volume_id,
        //https://github.com/protocolbuffers/protobuf/blob/master/src/google/protobuf/timestamp.proto
        creation_time: {
          seconds: zb.helpers.isPropertyValueSet(properties.creation.rawvalue)
            ? properties.creation.rawvalue
            : 0,
          nanos: 0,
        },
        ready_to_use: true,
      },
    };
  }

  /**
   * In addition, if clones have been created from a snapshot, then they must
   * be destroyed before the snapshot can be destroyed.
   *
   * @param {*} call
   */
  async DeleteSnapshot(call) {
    const driver = this;
    const httpApiClient = await this.getTrueNASHttpApiClient();
    const zb = await this.getZetabyte();

    const snapshot_id = call.request.snapshot_id;

    if (!snapshot_id) {
      throw new GrpcError(
        grpc.status.INVALID_ARGUMENT,
        `snapshot_id is required`
      );
    }

    const detachedSnapshot = !zb.helpers.isZfsSnapshot(snapshot_id);
    let datasetParentName;

    if (detachedSnapshot) {
      datasetParentName = this.getDetachedSnapshotParentDatasetName();
    } else {
      datasetParentName = this.getVolumeParentDatasetName();
    }

    if (!datasetParentName) {
      throw new GrpcError(
        grpc.status.FAILED_PRECONDITION,
        `invalid configuration: missing datasetParentName`
      );
    }

    const fullSnapshotName = datasetParentName + "/" + snapshot_id;

    driver.ctx.logger.verbose("deleting snapshot: %s", fullSnapshotName);

    if (detachedSnapshot) {
      try {
        await httpApiClient.DatasetDelete(fullSnapshotName, {
          recursive: true,
          force: true,
        });
      } catch (err) {
        throw err;
      }
    } else {
      try {
        await httpApiClient.SnapshotDelete(fullSnapshotName, {
          defer: true,
        });
      } catch (err) {
        if (err.toString().includes("snapshot has dependent clones")) {
          throw new GrpcError(
            grpc.status.FAILED_PRECONDITION,
            "snapshot has dependent clones"
          );
        }
        throw err;
      }
    }

    // cleanup parent dataset if possible
    if (detachedSnapshot) {
      let containerDataset =
        zb.helpers.extractParentDatasetName(fullSnapshotName);
      try {
        await this.removeSnapshotsFromDatatset(containerDataset);
        await httpApiClient.DatasetDelete(containerDataset);
      } catch (err) {
        if (!err.toString().includes("filesystem has children")) {
          throw err;
        }
      }
    }

    return {};
  }

  /**
   *
   * @param {*} call
   */
  async ValidateVolumeCapabilities(call) {
    const driver = this;
    const httpApiClient = await this.getTrueNASHttpApiClient();

    const volume_id = call.request.volume_id;
    if (!volume_id) {
      throw new GrpcError(grpc.status.INVALID_ARGUMENT, `missing volume_id`);
    }
    const capabilities = call.request.volume_capabilities;
    if (!capabilities || capabilities.length === 0) {
      throw new GrpcError(grpc.status.INVALID_ARGUMENT, `missing capabilities`);
    }

    let datasetParentName = this.getVolumeParentDatasetName();
    let name = volume_id;

    if (!datasetParentName) {
      throw new GrpcError(
        grpc.status.FAILED_PRECONDITION,
        `invalid configuration: missing datasetParentName`
      );
    }

    const datasetName = datasetParentName + "/" + name;
    try {
      await httpApiClient.DatasetGet(datasetName, []);
    } catch (err) {
      if (err.toString().includes("dataset does not exist")) {
        throw new GrpcError(
          grpc.status.NOT_FOUND,
          `invalid volume_id: ${volume_id}`
        );
      } else {
        throw err;
      }
    }

    const result = this.assertCapabilities(capabilities);

    if (result.valid !== true) {
      return { message: result.message };
    }

    return {
      confirmed: {
        volume_context: call.request.volume_context,
        volume_capabilities: call.request.volume_capabilities, // TODO: this is a bit crude, should return *ALL* capabilities, not just what was requested
        parameters: call.request.parameters,
      },
    };
  }
}

module.exports.TrueNASApiDriver = TrueNASApiDriver;
