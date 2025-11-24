const _ = require("lodash");
const { GrpcError, grpc } = require("../../utils/grpc");
const { CsiBaseDriver } = require("../index");
const JsonRpcClient = require("./jsonrpc/client").Client;
const TrueNASApiClient = require("./jsonrpc/api").Api;
const { Zetabyte } = require("../../utils/zfs");
const GeneralUtils = require("../../utils/general");

const Handlebars = require("handlebars");
const uuidv4 = require("uuid").v4;
const semver = require("semver");

// freenas properties
const FREENAS_NFS_SHARE_PROPERTY_NAME = "democratic-csi:freenas_nfs_share_id";
const FREENAS_SMB_SHARE_PROPERTY_NAME = "democratic-csi:freenas_smb_share_id";

// iscsi
const FREENAS_ISCSI_TARGET_ID_PROPERTY_NAME =
  "democratic-csi:freenas_iscsi_target_id";
const FREENAS_ISCSI_EXTENT_ID_PROPERTY_NAME =
  "democratic-csi:freenas_iscsi_extent_id";
const FREENAS_ISCSI_TARGETTOEXTENT_ID_PROPERTY_NAME =
  "democratic-csi:freenas_iscsi_targettoextent_id";
const FREENAS_ISCSI_ASSETS_NAME_PROPERTY_NAME =
  "democratic-csi:freenas_iscsi_assets_name";

// nvmeof
const FREENAS_NVMEOF_SUBSYSTEM_ID_PROPERTY_NAME =
  "democratic-csi:freenas_nvmeof_subsystem_id";
const FREENAS_NVMEOF_NAMESPACE_ID_PROPERTY_NAME =
  "democratic-csi:freenas_nvmeof_namespace_id";
const FREENAS_NVMEOF_ASSETS_NAME_PROPERTY_NAME =
  "democratic-csi:freenas_nvmeof_assets_name";

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

const __REGISTRY_NS__ = "FreeNASApiDriver";

class FreeNASApiDriver extends CsiBaseDriver {
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
              "cannot use the zb implementation to execute zfs commands, must use the api"
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
   */
  async createShare(call, datasetName) {
    const driver = this;
    const driverShareType = this.getDriverShareType();
    const client = await this.getJsonRpcClient();
    const apiClient = await this.getTrueNASApiClient();
    const zb = await this.getZetabyte();
    const truenasVersion = await apiClient.getSystemVersionSemver();

    let volume_context;
    let properties;
    let share = {};

    switch (driverShareType) {
      case "nfs":
        {
          properties = await apiClient.DatasetGet(datasetName, [
            "mountpoint",
            FREENAS_NFS_SHARE_PROPERTY_NAME,
          ]);
          this.ctx.logger.debug("zfs props data: %j", properties);

          // create nfs share
          if (
            !zb.helpers.isPropertyValueSet(
              properties[FREENAS_NFS_SHARE_PROPERTY_NAME].value
            )
          ) {
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

            share = {
              paths: [properties.mountpoint.value],
              comment: nfsShareComment || "",
              networks: this.options.nfs.shareAllowedNetworks,
              hosts: this.options.nfs.shareAllowedHosts,
              alldirs: this.options.nfs.shareAlldirs,
              ro: false,
              maproot_user: this.options.nfs.shareMaprootUser,
              maproot_group: this.options.nfs.shareMaprootGroup,
              mapall_user: this.options.nfs.shareMapallUser,
              mapall_group: this.options.nfs.shareMapallGroup,
              security: [],
            };

            let shareId;
            const res = await apiClient.SharingNfsCreate(share);
            shareId = res.id;

            //set zfs property
            await apiClient.DatasetSet(datasetName, {
              [FREENAS_NFS_SHARE_PROPERTY_NAME]: shareId,
            });
          }

          volume_context = {
            node_attach_driver: "nfs",
            server: this.options.nfs.shareHost,
            share: properties.mountpoint.value,
          };
          return volume_context;
        }
        break;
      case "smb":
        {
          properties = await apiClient.DatasetGet(datasetName, [
            "mountpoint",
            FREENAS_SMB_SHARE_PROPERTY_NAME,
          ]);
          this.ctx.logger.debug("zfs props data: %j", properties);

          let smbName;

          if (this.options.smb.nameTemplate) {
            smbName = Handlebars.compile(this.options.smb.nameTemplate)({
              name: call.request.name,
              parameters: call.request.parameters,
            });
          } else {
            smbName = zb.helpers.extractLeafName(datasetName);
          }

          if (this.options.smb.namePrefix) {
            smbName = this.options.smb.namePrefix + smbName;
          }

          if (this.options.smb.nameSuffix) {
            smbName += this.options.smb.nameSuffix;
          }

          smbName = smbName.toLowerCase();

          this.ctx.logger.info(
            "FreeNAS creating smb share with name: " + smbName
          );

          // create smb share
          if (
            !zb.helpers.isPropertyValueSet(
              properties[FREENAS_SMB_SHARE_PROPERTY_NAME].value
            )
          ) {
            share = {
                  name: smbName,
                  path: properties.mountpoint.value,
                };

                let propertyMapping = {
                  shareAuxiliaryConfigurationTemplate: "auxsmbconf",
                  shareHome: "home",
                  shareAllowedHosts: "hostsallow",
                  shareDeniedHosts: "hostsdeny",
                  shareDefaultPermissions: "default_permissions",
                  shareGuestOk: "guestok",
                  shareGuestOnly: "guestonly",
                  shareShowHiddenFiles: "showhiddenfiles",
                  shareRecycleBin: "recyclebin",
                  shareBrowsable: "browsable",
                  shareAccessBasedEnumeration: "abe",
                  shareTimeMachine: "timemachine",
                  shareStorageTask: "storage_task",
                };

                for (const key in propertyMapping) {
                  if (this.options.smb.hasOwnProperty(key)) {
                    let value;
                    switch (key) {
                      case "shareAuxiliaryConfigurationTemplate":
                        value = Handlebars.compile(
                          this.options.smb.shareAuxiliaryConfigurationTemplate
                        )({
                          name: call.request.name,
                          parameters: call.request.parameters,
                        });
                        break;
                      default:
                        value = this.options.smb[key];
                        break;
                    }
                    share[propertyMapping[key]] = value;
                  }
                }

                if (semver.satisfies(truenasVersion, ">=25.10")) {
                  let topLevelProperties = [
                    "purpose",
                    "name",
                    "path",
                    "enabled",
                    "comment",
                    "readonly",
                    "browsable",
                    "access_based_share_enumeration",
                    "audit",
                  ];
                  let disallowedOptions = ["abe"];
                  share.purpose = "LEGACY_SHARE";
                  share.options = {
                    purpose: "LEGACY_SHARE",
                  };
                  for (const key in share) {
                    switch (key) {
                      case "options":
                        // ignore
                        break;
                      default:
                        if (!topLevelProperties.includes(key)) {
                          if (!disallowedOptions.includes(key)) {
                            share.options[key] = share[key];
                          }
                          delete share[key];
                        }
                        break;
                    }
                  }
                }

                let shareId;
                const res = await apiClient.SharingSmbCreate(share);
                shareId = res.id;

                //set zfs property
                await apiClient.DatasetSet(datasetName, {
                  [FREENAS_SMB_SHARE_PROPERTY_NAME]: shareId,
                });
          }

          volume_context = {
            node_attach_driver: "smb",
            server: this.options.smb.shareHost,
            share: smbName,
          };
          return volume_context;
        }
        break;
      case "iscsi":
        {
          properties = await apiClient.DatasetGet(datasetName, [
            FREENAS_ISCSI_TARGET_ID_PROPERTY_NAME,
            FREENAS_ISCSI_EXTENT_ID_PROPERTY_NAME,
            FREENAS_ISCSI_TARGETTOEXTENT_ID_PROPERTY_NAME,
          ]);
          this.ctx.logger.debug("zfs props data: %j", properties);

          let basename;
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

          iscsiName = iscsiName.toLowerCase();

          let extentDiskName = "zvol/" + datasetName;
          let maxZvolNameLength = await driver.getMaxZvolNameLength();
          driver.ctx.logger.debug(
            "max zvol name length: %s",
            maxZvolNameLength
          );

          if (extentDiskName.length > maxZvolNameLength) {
            throw new GrpcError(
              grpc.status.FAILED_PRECONDITION,
              `extent disk name cannot exceed ${maxZvolNameLength} characters:  ${extentDiskName}`
            );
          }

          // https://github.com/SCST-project/scst/blob/master/scst/src/dev_handlers/scst_vdisk.c#L203
          if (iscsiName.length > 64) {
            throw new GrpcError(
              grpc.status.FAILED_PRECONDITION,
              `extent name cannot exceed 64 characters:  ${iscsiName}`
            );
          }

          this.ctx.logger.info(
            "FreeNAS creating iscsi assets with name: " + iscsiName
          );

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
            extentComment = "";
          }

          const extentInsecureTpc = this.options.iscsi.hasOwnProperty(
            "extentInsecureTpc"
          )
            ? this.options.iscsi.extentInsecureTpc
            : true;

          const extentXenCompat = this.options.iscsi.hasOwnProperty(
            "extentXenCompat"
          )
            ? this.options.iscsi.extentXenCompat
            : false;

          const extentBlocksize = this.options.iscsi.hasOwnProperty(
            "extentBlocksize"
          )
            ? this.options.iscsi.extentBlocksize
            : 512;

          const extentDisablePhysicalBlocksize =
            this.options.iscsi.hasOwnProperty("extentDisablePhysicalBlocksize")
              ? this.options.iscsi.extentDisablePhysicalBlocksize
              : true;

          const extentRpm = this.options.iscsi.hasOwnProperty("extentRpm")
            ? this.options.iscsi.extentRpm
            : "SSD";

          let extentAvailThreshold = this.options.iscsi.hasOwnProperty(
            "extentAvailThreshold"
          )
            ? Number(this.options.iscsi.extentAvailThreshold)
            : null;

          if (!(extentAvailThreshold > 0 && extentAvailThreshold <= 100)) {
            extentAvailThreshold = null;
          }

          const config = await apiClient.IscsiGetGlobalConfig();
          basename = config.basename;
          this.ctx.logger.verbose("FreeNAS ISCSI BASENAME: " + basename);

          // TARGET
          if (
            !zb.helpers.isPropertyValueSet(
              properties[FREENAS_ISCSI_TARGETTOEXTENT_ID_PROPERTY_NAME].value
            )
          ) {
                let targetGroups = [];
                for (let targetGroupConfig of this.options.iscsi.targetGroups) {
                  targetGroups.push({
                    portal: targetGroupConfig.targetGroupPortalGroup,
                    initiator: targetGroupConfig.targetGroupInitiatorGroup,
                    auth:
                      targetGroupConfig.targetGroupAuthGroup > 0
                        ? targetGroupConfig.targetGroupAuthGroup
                        : null,
                    authmethod:
                      targetGroupConfig.targetGroupAuthType.length > 0
                        ? targetGroupConfig.targetGroupAuthType
                            .toUpperCase()
                            .replace(" ", "_")
                        : "NONE",
                  });
                }

                let target;
                let targetId;
                let targetData = {
                  name: iscsiName,
                  alias: null,
                  mode: "ISCSI",
                  groups: targetGroups,
                };

                target = await apiClient.IscsiTargetCreate(targetData);
                targetId = target.id;

                // Update groups if needed and target already existed
                if (target.groups && target.groups.length != targetGroups.length) {
                    target = await apiClient.IscsiTargetUpdate(targetId, { groups: targetGroups });
                }

                await apiClient.DatasetSet(datasetName, {
                  [FREENAS_ISCSI_TARGET_ID_PROPERTY_NAME]: targetId,
                });

                // EXTENT
                let extent;
                let extentId;
                let extentData = {
                  comment: extentComment,
                  type: "DISK",
                  name: iscsiName,
                  disk: extentDiskName,
                  insecure_tpc: extentInsecureTpc,
                  xen: extentXenCompat,
                  avail_threshold: extentAvailThreshold,
                  blocksize: Number(extentBlocksize),
                  pblocksize: extentDisablePhysicalBlocksize,
                  rpm: "" + extentRpm,
                  ro: false,
                };

                extent = await apiClient.IscsiExtentCreate(extentData);
                extentId = extent.id;

                await apiClient.DatasetSet(datasetName, {
                  [FREENAS_ISCSI_EXTENT_ID_PROPERTY_NAME]: extentId,
                });

                // TARGET EXTENT
                let targetToExtent;
                let targetToExtentId;
                let targetToExtentData = {
                  target: targetId,
                  extent: extentId,
                  lunid: 0,
                };

                targetToExtent = await apiClient.IscsiTargetExtentCreate(targetToExtentData);
                targetToExtentId = targetToExtent.id;

                await apiClient.DatasetSet(datasetName, {
                  [FREENAS_ISCSI_TARGETTOEXTENT_ID_PROPERTY_NAME]:
                    targetToExtentId,
                });
          }

          // iqn = target
          let iqn = basename + ":" + iscsiName;
          this.ctx.logger.info("FreeNAS iqn: " + iqn);

          // store this off to make delete process more bullet proof
          await apiClient.DatasetSet(datasetName, {
            [FREENAS_ISCSI_ASSETS_NAME_PROPERTY_NAME]: iscsiName,
          });

          volume_context = {
            node_attach_driver: "iscsi",
            portal: this.options.iscsi.targetPortal || "",
            portals: this.options.iscsi.targetPortals
              ? this.options.iscsi.targetPortals.join(",")
              : "",
            interface: this.options.iscsi.interface || "",
            iqn: iqn,
            lun: 0,
          };
          return volume_context;
        }
        break;

      case "nvmeof":
        {
          properties = await apiClient.DatasetGet(datasetName, [
            FREENAS_NVMEOF_SUBSYSTEM_ID_PROPERTY_NAME,
            FREENAS_NVMEOF_NAMESPACE_ID_PROPERTY_NAME,
            FREENAS_NVMEOF_ASSETS_NAME_PROPERTY_NAME,
          ]);
          this.ctx.logger.debug("zfs props data: %j", properties);

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

          nvmeofName = nvmeofName.toLowerCase();

          let namespaceDiskName = "zvol/" + datasetName;
          let maxZvolNameLength = await driver.getMaxZvolNameLength();

          if (namespaceDiskName.length > maxZvolNameLength) {
            throw new GrpcError(
              grpc.status.FAILED_PRECONDITION,
              `namespace disk name cannot exceed ${maxZvolNameLength} characters: ${namespaceDiskName}`
            );
          }

          this.ctx.logger.info(
            "FreeNAS creating nvmeof assets with name: " + nvmeofName
          );

          // http://<ip>/api/docs/current/api_methods_nvmet.subsys.create.html
          let subsystemTemplate = _.get(
            this.options,
            "nvmeof.subsystemTemplate",
            {}
          );
          subsystemTemplate = subsystemTemplate || {};

          // http://<ip>/api/docs/current/api_methods_nvmet.namespace.create.html
          let namespaceTemplate = _.get(
            this.options,
            "nvmeof.namespaceTemplate",
            {}
          );
          namespaceTemplate = namespaceTemplate || {};

          // create subsystem
          const subsystem = await apiClient.NvmetSubsysCreate(
            nvmeofName,
            subsystemTemplate
          );

          if (!subsystem) {
            throw new GrpcError(
              grpc.status.NOT_FOUND,
              `unable to find nvmeof subsystem: ${nvmeofName}`
            );
          }
          this.ctx.logger.verbose("FreeNAS NVMEOF SUBSYSTEM: %j", subsystem);
          await apiClient.DatasetSet(datasetName, {
            [FREENAS_NVMEOF_SUBSYSTEM_ID_PROPERTY_NAME]: subsystem.id,
          });

          // create namespace
          const namespace = await apiClient.NvmetNamespaceCreate(
            namespaceDiskName,
            subsystem.id,
            namespaceTemplate
          );

          if (!namespace) {
            throw new GrpcError(
              grpc.status.NOT_FOUND,
              `unable to find nvmeof namespace: ${namespaceDiskName}`
            );
          }
          this.ctx.logger.verbose("FreeNAS NVMEOF NAMESPACE: %j", namespace);
          await apiClient.DatasetSet(datasetName, {
            [FREENAS_NVMEOF_NAMESPACE_ID_PROPERTY_NAME]: namespace.id,
          });

          // assign ports to subsystem
          let ports = _.get(this.options, "nvmeof.ports", []);
          for (const port_i of ports) {
            const port = await apiClient.NvmetPortSubsysCreate(
              port_i,
              subsystem.id
            );
            this.ctx.logger.verbose("FreeNAS NVMEOF PORT: %j", port);
          }

          // store this off to make delete process more bullet proof
          await apiClient.DatasetSet(datasetName, {
            [FREENAS_NVMEOF_ASSETS_NAME_PROPERTY_NAME]: nvmeofName,
          });

          volume_context = {
            node_attach_driver: "nvmeof",
            transport: this.options.nvmeof.transport || "",
            transports: this.options.nvmeof.transports
              ? this.options.nvmeof.transports.join(",")
              : "",
            nqn: subsystem.subnqn,
            nsid: namespace.nsid,
          };
          return volume_context;
        }
        break;

      default:
        throw new GrpcError(
          grpc.status.FAILED_PRECONDITION,
          `invalid configuration: unknown driverShareType ${driverShareType}`
        );
    }
  }

  async deleteShare(call, datasetName) {
    const driverShareType = this.getDriverShareType();
    const client = await this.getJsonRpcClient();
    const apiClient = await this.getTrueNASApiClient();
    const zb = await this.getZetabyte();

    let properties;
    let shareId;

    switch (driverShareType) {
      case "nfs":
        {
          try {
            properties = await apiClient.DatasetGet(datasetName, [
              "mountpoint",
              FREENAS_NFS_SHARE_PROPERTY_NAME,
            ]);
          } catch (err) {
            if (err.toString().includes("dataset does not exist")) {
              return;
            }
            throw err;
          }
          this.ctx.logger.debug("zfs props data: %j", properties);

          shareId = properties[FREENAS_NFS_SHARE_PROPERTY_NAME].value;

          if (zb.helpers.isPropertyValueSet(shareId)) {
             await apiClient.SharingNfsDelete(shareId);
             await apiClient.DatasetInherit(
               datasetName,
               FREENAS_NFS_SHARE_PROPERTY_NAME
             );
          }
        }
        break;
      case "smb":
        {
          try {
            properties = await apiClient.DatasetGet(datasetName, [
              "mountpoint",
              FREENAS_SMB_SHARE_PROPERTY_NAME,
            ]);
          } catch (err) {
            if (err.toString().includes("dataset does not exist")) {
              return;
            }
            throw err;
          }
          this.ctx.logger.debug("zfs props data: %j", properties);

          shareId = properties[FREENAS_SMB_SHARE_PROPERTY_NAME].value;

          if (zb.helpers.isPropertyValueSet(shareId)) {
             await apiClient.SharingSmbDelete(shareId);
             await apiClient.DatasetInherit(
               datasetName,
               FREENAS_SMB_SHARE_PROPERTY_NAME
             );
          }
        }
        break;
      case "iscsi":
        {
          try {
            properties = await apiClient.DatasetGet(datasetName, [
              FREENAS_ISCSI_TARGET_ID_PROPERTY_NAME,
              FREENAS_ISCSI_EXTENT_ID_PROPERTY_NAME,
              FREENAS_ISCSI_TARGETTOEXTENT_ID_PROPERTY_NAME,
              FREENAS_ISCSI_ASSETS_NAME_PROPERTY_NAME,
            ]);
          } catch (err) {
            if (err.toString().includes("dataset does not exist")) {
              return;
            }
            throw err;
          }

          this.ctx.logger.debug("zfs props data: %j", properties);

          let targetId =
            properties[FREENAS_ISCSI_TARGET_ID_PROPERTY_NAME].value;
          let extentId =
            properties[FREENAS_ISCSI_EXTENT_ID_PROPERTY_NAME].value;

          if (zb.helpers.isPropertyValueSet(targetId)) {
              try {
                  await apiClient.IscsiTargetDelete(targetId);
              } catch(err) {
                 if (err.toString().includes("is in use") || (err.error && err.error === 14)) {
                     // Retry if in use
                     await GeneralUtils.retry(5, 1000, async () => {
                         await apiClient.IscsiTargetDelete(targetId);
                     });
                 } else {
                     throw err;
                 }
              }
              await apiClient.DatasetInherit(
                 datasetName,
                 FREENAS_ISCSI_TARGET_ID_PROPERTY_NAME
              );
          }

          if (zb.helpers.isPropertyValueSet(extentId)) {
              await apiClient.IscsiExtentDelete(extentId);
              await apiClient.DatasetInherit(
                 datasetName,
                 FREENAS_ISCSI_EXTENT_ID_PROPERTY_NAME
              );
          }
        }
        break;

      case "nvmeof":
        {
          try {
            properties = await apiClient.DatasetGet(datasetName, [
              FREENAS_NVMEOF_SUBSYSTEM_ID_PROPERTY_NAME,
              FREENAS_NVMEOF_NAMESPACE_ID_PROPERTY_NAME,
              FREENAS_NVMEOF_ASSETS_NAME_PROPERTY_NAME,
            ]);
          } catch (err) {
            if (err.toString().includes("dataset does not exist")) {
              return;
            }
            throw err;
          }

          this.ctx.logger.debug("zfs props data: %j", properties);

          let subsystemId =
            properties[FREENAS_NVMEOF_SUBSYSTEM_ID_PROPERTY_NAME].value;
          let namespaceId =
            properties[FREENAS_NVMEOF_NAMESPACE_ID_PROPERTY_NAME].value;

          if (zb.helpers.isPropertyValueSet(namespaceId)) {
             await apiClient.NvmetNamespaceDeleteById(namespaceId);
            await apiClient.DatasetInherit(
              datasetName,
              FREENAS_NVMEOF_NAMESPACE_ID_PROPERTY_NAME
            );
          }

          if (zb.helpers.isPropertyValueSet(subsystemId)) {
             await apiClient.NvmetSubsysDeleteById(subsystemId);
            await apiClient.DatasetInherit(
              datasetName,
              FREENAS_NVMEOF_SUBSYSTEM_ID_PROPERTY_NAME
            );
          }
        }
        break;

      default:
        throw new GrpcError(
          grpc.status.FAILED_PRECONDITION,
          `invalid configuration: unknown driverShareType ${driverShareType}`
        );
    }
  }

  async removeSnapshotsFromDatatset(datasetName) {
    const apiClient = await this.getTrueNASApiClient();
    await apiClient.DatasetDestroySnapshots(datasetName);
  }

  /**
   * Expand volume notification to the storage backend.
   * For iSCSI, we need to reload the service so the target reports the new size.
   *
   * @param {*} call
   * @param {*} datasetName
   * @returns
   */
  async expandVolume(call, datasetName) {
    const driverShareType = this.getDriverShareType();
    const apiClient = await this.getTrueNASApiClient();

    switch (driverShareType) {
      case "iscsi":
          // Reload iSCSI service to update extent sizes
          // SCALE middleware should handle this automatically in most cases,
          // but we force a reload to ensure consistency
          await apiClient.ServiceReload("iscsitarget");
        break;
      // NFS and SMB don't require service reload for size changes
      case "nfs":
      case "smb":
      case "nvmeof":
        // No action needed
        break;
    }
  }

  /**
   * Get the max size a zvol name can be
   */
  async getMaxZvolNameLength() {
    return 255; // SCALE always
  }

  async getJsonRpcClient() {
    return this.ctx.registry.getAsync(
      `${__REGISTRY_NS__}:jsonrpc_client`,
      async () => {
        const client = new JsonRpcClient({
            ...this.options.httpConnection,
            logger: this.ctx.logger
        });
        // Register cleanup handler to close WebSocket connection
        this.cleanup.push(() => client.close());
        return client;
      }
    );
  }

  async getTrueNASApiClient() {
    return this.ctx.registry.getAsync(
      `${__REGISTRY_NS__}:api_client`,
      async () => {
        const client = await this.getJsonRpcClient();
        return new TrueNASApiClient(client, this.ctx.cache);
      }
    );
  }

  // ... Methods like Probe, CreateVolume, DeleteVolume, etc use getTrueNASApiClient and are mostly compatible if apiClient has same methods.

  async Probe(callContext, call) {
    const driver = this;
    const apiClient = await driver.getTrueNASApiClient();

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
        await apiClient.getSystemVersion();
      } catch (err) {
        throw new GrpcError(
          grpc.status.FAILED_PRECONDITION,
          `TrueNAS api is unavailable: ${String(err)}`
        );
      }

      return CsiBaseDriver.prototype.Probe.call(this, ...arguments);
    } else {
      return CsiBaseDriver.prototype.Probe.call(this, ...arguments);
    }
  }

  async getMinimumVolumeSize() {
    const driverZfsResourceType = this.getDriverZfsResourceType();
    switch (driverZfsResourceType) {
      case "filesystem":
        return 1073741824;
    }
  }
}

module.exports.FreeNASApiDriver = FreeNASApiDriver;
