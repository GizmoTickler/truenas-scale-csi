const { TrueNASApiDriver } = require("./truenas/api");

function factory(ctx, options) {
  switch (options.driver) {
    // TrueNAS SCALE 25.04+ drivers using JSON-RPC over WebSocket
    case "truenas-nfs":
    case "truenas-iscsi":
    case "truenas-nvmeof":
      return new TrueNASApiDriver(ctx, options);
    default:
      throw new Error("invalid csi driver: " + options.driver + ". Only truenas-nfs, truenas-iscsi, and truenas-nvmeof are supported.");
  }
}

module.exports.factory = factory;
