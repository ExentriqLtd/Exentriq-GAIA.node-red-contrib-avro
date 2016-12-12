module.exports = function(RED) {

    function AvroFromBuffer(config) {
        RED.nodes.createNode(this, config);
        var node = this;
        var avro = require('avsc');

        var jsonSchema = JSON.parse(config.schema);
        var avroType = avro.parse(jsonSchema, {"wrapUnions":false});

        try {
            this.on('input', function(msg) {
                var deserialized = avroType.fromBuffer(msg.payload);
                msg.payload=deserialized;
                node.send(msg);
            });
        } catch (e) {
            node.error("ops, there was an error!", msg);
        }
    }
    
    function AvroToBuffer(config) {
        RED.nodes.createNode(this, config);
        var node = this;
        var avro = require('avsc');

        var jsonSchema = JSON.parse(config.schema);
        var avroType = avro.parse(jsonSchema, {"wrapUnions":false});

        try {
            this.on('input', function(msg) {
                var serialized = avroType.toBuffer(msg.payload);
                msg.payload=serialized;
                node.send(msg);
            });
        } catch (e) {
            node.error("ops, there was an error!", msg);
        }
    }

    RED.nodes.registerType("avro-from-buffer", AvroFromBuffer);
    RED.nodes.registerType("avro-to-buffer", AvroToBuffer);
}