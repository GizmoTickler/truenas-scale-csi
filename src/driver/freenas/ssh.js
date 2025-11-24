const _ = require("lodash");
const { ControllerZfsBaseDriver } = require("../controller-zfs");
const { GrpcError, grpc } = require("../../utils/grpc");
const SshClient = require("../../utils/zfs_ssh_exec_client").SshClient;
const JsonRpcClient = require("./jsonrpc/client").Client;
const TrueNASApiClient = require("./jsonrpc/api").Api;
const { Zetabyte, ZfsSshProcessManager } = require("../../utils/zfs");
const GeneralUtils = require("../../utils/general");

const Handlebars = require("handlebars");
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

// used for in-memory cache of the version info
const FREENAS_SYSTEM_VERSION_CACHE_KEY = "freenas:system_version";
const __REGISTRY_NS__ = "FreeNASSshDriver";

class FreeNASSshDriver extends ControllerZfsBaseDriver {
  /**
   * Ensure sane options are used etc
   * true = ready
   * false = not ready, but progressiong towards ready
   * throw error = faulty setup
   *
   * @param {*} call
   */
  async Probe(callContext, call) {
    const driver = this;

    if (driver.ctx.args.csiMode.includes("controller")) {
      const apiClient = await driver.getTrueNASApiClient();
      try {
        await apiClient.getSystemVersion();
      } catch (err) {
        throw new GrpcError(
          grpc.status.FAILED_PRECONDITION,
          `TrueNAS api is unavailable: ${String(err)}`
        );
      }

      return super.Probe(...arguments);
    } else {
      return super.Probe(...arguments);
    }
  }

  getExecClient() {
    return this.ctx.registry.get(`${__REGISTRY_NS__}:exec_client`, () => {
      const sshClient = new SshClient({
        logger: this.ctx.logger,
        connection: this.options.sshConnection,
      });
      this.cleanup.push(() => sshClient.finalize());
      return sshClient;
    });
  }

  async getZetabyte() {
    return this.ctx.registry.getAsync(`${__REGISTRY_NS__}:zb`, async () => {
      const sshClient = this.getExecClient();
      const options = {};
      options.executor = new ZfsSshProcessManager(sshClient);
      options.idempotent = true;
      options.sudo = _.get(this.options, "zfs.cli.sudoEnabled", false);
      const sudoEnabledCommands = _.get(
        this.options,
        "zfs.cli.sudoEnabledCommands"
      );
      if (sudoEnabledCommands && Array.isArray(sudoEnabledCommands)) {
        options.sudo = {};
        sudoEnabledCommands.forEach((command) => {
          options.sudo[command] = true;
        });
      }

      if (typeof this.setZetabyteCustomOptions === "function") {
        await this.setZetabyteCustomOptions(options);
      }

      options.paths = options.paths || {};
      options.paths = Object.assign(
        {},
        options.paths,
        _.get(this.options, "zfs.cli.paths", {})
      );

      return new Zetabyte(options);
    });
  }

  /**
   * cannot make this a storage class parameter as storage class/etc context is *not* sent
   * into various calls such as GetControllerCapabilities etc
   */
  getDriverZfsResourceType() {
    switch (this.options.driver) {
      case "freenas-nfs":
      case "truenas-nfs":
      case "freenas-smb":
      case "truenas-smb":
        return "filesystem";
      case "freenas-iscsi":
      case "truenas-iscsi":
      case "freenas-nvmeof":
      case "truenas-nvmeof":
        return "volume";
      default:
        throw new Error("unknown driver: " + this.options.driver);
    }
  }

  async setZetabyteCustomOptions(options) {
    // We are only supporting SCALE 25.04+
    options.paths = {
      zfs: "/usr/sbin/zfs",
      zpool: "/usr/sbin/zpool",
      sudo: "/usr/bin/sudo",
      chroot: "/usr/sbin/chroot",
    };
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

  getDriverShareType() {
    switch (this.options.driver) {
      case "freenas-nfs":
      case "truenas-nfs":
        return "nfs";
      case "freenas-smb":
      case "truenas-smb":
        return "smb";
      case "freenas-iscsi":
      case "truenas-iscsi":
        return "iscsi";
      case "freenas-nvmeof":
      case "truenas-nvmeof":
        return "nvmeof";
      default:
        throw new Error("unknown driver: " + this.options.driver);
    }
  }

  /**
   * should create any necessary share resources
   * should set the SHARE_VOLUME_CONTEXT_PROPERTY_NAME propery
   *
   * @param {*} datasetName
   */
  async createShare(callContext, call, datasetName) {
    const driver = this;
    const driverShareType = this.getDriverShareType();
    const client = await this.getJsonRpcClient();
    const apiClient = await this.getTrueNASApiClient();
    const zb = await this.getZetabyte();

    let volume_context;
    let properties;
    let share = {};

    switch (driverShareType) {
      case "nfs":
        {
          properties = await zb.zfs.get(datasetName, [
            "mountpoint",
            FREENAS_NFS_SHARE_PROPERTY_NAME,
          ]);
          properties = properties[datasetName];
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

            // SCALE/25.04 adjustments if needed.
            // `path` vs `paths`: documentation says `paths`.

            let shareId;
            try {
                const res = await client.call("sharing.nfs.create", [share]);
                shareId = res.id;
            } catch (err) {
                // Check if already exists
                if (err.toString().includes("already exists") || (err.error && err.error === 17) || (JSON.stringify(err).includes("exporting this path"))) {
                    // Try to find it
                    const existing = await client.call("sharing.nfs.query", [[["paths", "in", [properties.mountpoint.value]]]]);
                    if (existing && existing.length > 0) {
                        shareId = existing[0].id;
                    } else {
                         throw err;
                    }
                } else {
                    throw err;
                }
            }

            //set zfs property
            await zb.zfs.set(datasetName, {
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
          properties = await zb.zfs.get(datasetName, [
            "mountpoint",
            FREENAS_SMB_SHARE_PROPERTY_NAME,
          ]);
          properties = properties[datasetName];
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

                // Map options
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

                // Scale 25.10+ cleanup for legacy share purpose
                // As per previous logic
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

                let shareId;
                try {
                    const res = await client.call("sharing.smb.create", [share]);
                    shareId = res.id;
                } catch(err) {
                    if (err.toString().includes("already exists") || (err.error && err.error === 17)) {
                        const existing = await client.call("sharing.smb.query", [[["name", "=", smbName]]]);
                        if (existing && existing.length > 0) {
                            shareId = existing[0].id;
                        } else {
                            throw err;
                        }
                    } else {
                        throw err;
                    }
                }

                await zb.zfs.set(datasetName, {
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
          properties = await zb.zfs.get(datasetName, [
            FREENAS_ISCSI_TARGET_ID_PROPERTY_NAME,
            FREENAS_ISCSI_EXTENT_ID_PROPERTY_NAME,
            FREENAS_ISCSI_TARGETTOEXTENT_ID_PROPERTY_NAME,
          ]);
          properties = properties[datasetName];
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

          if (extentDiskName.length > maxZvolNameLength) {
            throw new GrpcError(
              grpc.status.FAILED_PRECONDITION,
              `extent disk name cannot exceed ${maxZvolNameLength} characters: ${extentDiskName}`
            );
          }

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

          const config = await client.call("iscsi.global.config");
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

                try {
                    target = await client.call("iscsi.target.create", [targetData]);
                    targetId = target.id;
                } catch(err) {
                     if (err.toString().includes("already exists") || (err.error && err.error === 17)) {
                         const existing = await client.call("iscsi.target.query", [[["name", "=", iscsiName]]]);
                         if (existing && existing.length > 0) {
                             target = existing[0];
                             targetId = target.id;

                             // Update groups if needed
                             if (target.groups.length != targetGroups.length) {
                                 target = await client.call("iscsi.target.update", [targetId, { groups: targetGroups }]);
                             }
                         } else {
                             throw err;
                         }
                     } else {
                         throw err;
                     }
                }

                await zb.zfs.set(datasetName, {
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

                try {
                    extent = await client.call("iscsi.extent.create", [extentData]);
                    extentId = extent.id;
                } catch (err) {
                    if (err.toString().includes("already exists") || (err.error && err.error === 17)) {
                         const existing = await client.call("iscsi.extent.query", [[["name", "=", iscsiName]]]);
                         if (existing && existing.length > 0) {
                             extent = existing[0];
                             extentId = extent.id;
                         } else {
                             throw err;
                         }
                    } else {
                        throw err;
                    }
                }

                await zb.zfs.set(datasetName, {
                  [FREENAS_ISCSI_EXTENT_ID_PROPERTY_NAME]: extentId,
                });

                // TARGET EXTENT
                let targetToExtent;
                let targetToExtentId;
                let targetToExtentData = {
                  target: targetId,
                  extent: extentId,
                  lunid: 0, // 0 for now
                };

                try {
                    targetToExtent = await client.call("iscsi.targetextent.create", [targetToExtentData]);
                    targetToExtentId = targetToExtent.id;
                } catch(err) {
                    if (err.toString().includes("already exists") || (err.error && err.error === 17) || JSON.stringify(err).includes("already in this target") || JSON.stringify(err).includes("LUN ID is already being used")) {
                        const existing = await client.call("iscsi.targetextent.query", [[["target", "=", targetId], ["extent", "=", extentId]]]);
                        if (existing && existing.length > 0) {
                             targetToExtent = existing[0];
                             targetToExtentId = targetToExtent.id;
                         } else {
                             // Fallback lookup if filters differ?
                             throw err;
                         }
                    } else {
                        throw err;
                    }
                }

                await zb.zfs.set(datasetName, {
                  [FREENAS_ISCSI_TARGETTOEXTENT_ID_PROPERTY_NAME]:
                    targetToExtentId,
                });
          }

          // iqn = target
          let iqn = basename + ":" + iscsiName;
          this.ctx.logger.info("FreeNAS iqn: " + iqn);

          // store this off to make delete process more bullet proof
          await zb.zfs.set(datasetName, {
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
          properties = await zb.zfs.get(datasetName, [
            FREENAS_NVMEOF_SUBSYSTEM_ID_PROPERTY_NAME,
            FREENAS_NVMEOF_NAMESPACE_ID_PROPERTY_NAME,
            FREENAS_NVMEOF_ASSETS_NAME_PROPERTY_NAME,
          ]);
          properties = properties[datasetName];
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

          let subsystemTemplate = _.get(
            this.options,
            "nvmeof.subsystemTemplate",
            {}
          );
          subsystemTemplate = subsystemTemplate || {};

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
          await zb.zfs.set(datasetName, {
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
          await zb.zfs.set(datasetName, {
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
          await zb.zfs.set(datasetName, {
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

  async deleteShare(callContext, call, datasetName) {
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
            properties = await zb.zfs.get(datasetName, [
              "mountpoint",
              FREENAS_NFS_SHARE_PROPERTY_NAME,
            ]);
          } catch (err) {
            if (err.toString().includes("dataset does not exist")) {
              return;
            }
            throw err;
          }
          properties = properties[datasetName];
          this.ctx.logger.debug("zfs props data: %j", properties);

          shareId = properties[FREENAS_NFS_SHARE_PROPERTY_NAME].value;

          if (zb.helpers.isPropertyValueSet(shareId)) {
             try {
                await client.call("sharing.nfs.delete", [shareId]);
             } catch(err) {
                 if (err.toString().includes("does not exist") || (err.error && err.error === 2)) {
                     // OK
                 } else {
                     throw err;
                 }
             }
             await zb.zfs.inherit(
               datasetName,
               FREENAS_NFS_SHARE_PROPERTY_NAME
             );
          }
        }
        break;
      case "smb":
        {
          try {
            properties = await zb.zfs.get(datasetName, [
              "mountpoint",
              FREENAS_SMB_SHARE_PROPERTY_NAME,
            ]);
          } catch (err) {
            if (err.toString().includes("dataset does not exist")) {
              return;
            }
            throw err;
          }
          properties = properties[datasetName];
          this.ctx.logger.debug("zfs props data: %j", properties);

          shareId = properties[FREENAS_SMB_SHARE_PROPERTY_NAME].value;

          if (zb.helpers.isPropertyValueSet(shareId)) {
             try {
                await client.call("sharing.smb.delete", [shareId]);
             } catch(err) {
                 if (err.toString().includes("does not exist") || (err.error && err.error === 2)) {
                     // OK
                 } else {
                     throw err;
                 }
             }
             await zb.zfs.inherit(
               datasetName,
               FREENAS_SMB_SHARE_PROPERTY_NAME
             );
          }
        }
        break;
      case "iscsi":
        {
          try {
            properties = await zb.zfs.get(datasetName, [
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

          properties = properties[datasetName];
          this.ctx.logger.debug("zfs props data: %j", properties);

          let targetId =
            properties[FREENAS_ISCSI_TARGET_ID_PROPERTY_NAME].value;
          let extentId =
            properties[FREENAS_ISCSI_EXTENT_ID_PROPERTY_NAME].value;

          if (zb.helpers.isPropertyValueSet(targetId)) {
              // Delete target
              try {
                  await client.call("iscsi.target.delete", [targetId]);
              } catch(err) {
                 if (err.toString().includes("does not exist") || (err.error && err.error === 2)) {
                     // OK
                 } else if (err.toString().includes("is in use") || (err.error && err.error === 14)) {
                     // Retry logic?
                     // original used retries.
                     await GeneralUtils.retry(5, 1000, async () => {
                         await client.call("iscsi.target.delete", [targetId]);
                     });
                 } else {
                     throw err;
                 }
              }
              await zb.zfs.inherit(
                 datasetName,
                 FREENAS_ISCSI_TARGET_ID_PROPERTY_NAME
              );
          }

          if (zb.helpers.isPropertyValueSet(extentId)) {
              try {
                  await client.call("iscsi.extent.delete", [extentId]);
              } catch(err) {
                 if (err.toString().includes("does not exist") || (err.error && err.error === 2)) {
                     // OK
                 } else {
                     throw err;
                 }
              }
              await zb.zfs.inherit(
                 datasetName,
                 FREENAS_ISCSI_EXTENT_ID_PROPERTY_NAME
              );
          }
        }
        break;

      case "nvmeof":
        {
          try {
            properties = await zb.zfs.get(datasetName, [
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

          properties = properties[datasetName];
          this.ctx.logger.debug("zfs props data: %j", properties);

          let subsystemId =
            properties[FREENAS_NVMEOF_SUBSYSTEM_ID_PROPERTY_NAME].value;
          let namespaceId =
            properties[FREENAS_NVMEOF_NAMESPACE_ID_PROPERTY_NAME].value;

          // remove namespace
          if (zb.helpers.isPropertyValueSet(namespaceId)) {
             await apiClient.NvmetNamespaceDeleteById(namespaceId);
            await zb.zfs.inherit(
              datasetName,
              FREENAS_NVMEOF_NAMESPACE_ID_PROPERTY_NAME
            );
          }

          // remove subsystem
          if (zb.helpers.isPropertyValueSet(subsystemId)) {
             await apiClient.NvmetSubsysDeleteById(subsystemId);
            await zb.zfs.inherit(
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

  async setFilesystemMode(path, mode) {
      const apiClient = await this.getTrueNASApiClient();
      let perms = {
          path,
          mode: String(mode),
      };
      await apiClient.FilesystemSetperm(perms);
  }

  async setFilesystemOwnership(path, user = false, group = false) {
    const apiClient = await this.getTrueNASApiClient();

    if (user === false || typeof user == "undefined" || user === null) {
      user = "";
    }

    if (group === false || typeof group == "undefined" || group === null) {
      group = "";
    }

    user = String(user);
    group = String(group);

    if (user.length < 1 && group.length < 1) {
      return;
    }

    let perms = {
          path,
    };
    if (user.length > 0) perms.uid = Number(user);
    if (group.length > 0) perms.gid = Number(group);

    await apiClient.FilesystemSetperm(perms);
  }

  async expandVolume(callContext, call, datasetName) {
    const driverShareType = this.getDriverShareType();
    const execClient = this.getExecClient();
    const client = await this.getJsonRpcClient();
    const zb = await this.getZetabyte();

    switch (driverShareType) {
      case "iscsi":
          let properties;
          properties = await zb.zfs.get(datasetName, [
            FREENAS_ISCSI_ASSETS_NAME_PROPERTY_NAME,
          ]);
          properties = properties[datasetName];
          this.ctx.logger.debug("zfs props data: %j", properties);
          let iscsiName =
            properties[FREENAS_ISCSI_ASSETS_NAME_PROPERTY_NAME].value;

          let kName = iscsiName.replaceAll(".", "_");
          let command = execClient.buildCommand("sh", [
            "-c",
            `"echo 1 > /sys/kernel/scst_tgt/devices/${kName}/resync_size"`,
          ]);

          if ((await this.getWhoAmI()) != "root") {
            command = (await this.getSudoPath()) + " " + command;
          }

          this.ctx.logger.verbose(
            "FreeNAS reloading iscsi lun size: %s",
            command
          );

          let response = await execClient.exec(command);
          if (response.code != 0) {
              // Try service reload as fallback?
              // The original logic only did this if NOT SCALE?
              // Wait, original logic: if isScale... else ... reload.
              // We are always SCALE 25.04.

              // If sysfs method fails, maybe try service reload?
              // But sysfs is for SCST which SCALE uses.
              // If it fails, log and try reload?
              this.ctx.logger.warn("Failed to resync lun size via sysfs, trying service reload");

              await client.call("service.reload", ["iscsitarget"]);
          }
        break;
    }
  }
}

module.exports.FreeNASSshDriver = FreeNASSshDriver;
