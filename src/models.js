const mongoose = require("mongoose");
const { Schema } = mongoose;

// These schemas mirror src/models/Mqtt*.ts in the Next.js app field-for-field.
// Model names must match exactly — Mongoose derives the collection name
// (pluralized, lowercased) from the model name, and both services need to
// land on the same collections without either one owning the "real" schema.

const MqttUserSchema = new Schema(
  {
    userId:        { type: Schema.Types.ObjectId, ref: "User", required: true, index: true },
    username:      { type: String, required: true, unique: true, trim: true, lowercase: true },
    password:      { type: String, required: true },
    isActive:      { type: Boolean, default: true },
    maxConnection: { type: Number, default: 3, min: 1 },
  },
  { timestamps: true }
);

const MqttClientSchema = new Schema(
  {
    userId:     { type: Schema.Types.ObjectId, ref: "User",     required: true, index: true },
    mqttUserId: { type: Schema.Types.ObjectId, ref: "MqttUser", required: true, index: true },
    clientName: { type: String, required: true, trim: true },
    isOnline:   { type: Boolean, default: false },
    lastSeenAt: { type: Date, default: null },
  },
  { timestamps: true }
);
MqttClientSchema.index({ mqttUserId: 1, clientName: 1 }, { unique: true });

const MqttActivitySchema = new Schema(
  {
    userId:     { type: Schema.Types.ObjectId, ref: "User",     required: true, index: true },
    mqttUserId: { type: Schema.Types.ObjectId, ref: "MqttUser", required: true, index: true },
    clientName: { type: String, required: true },
    event:      { type: String, enum: ["connect", "disconnect"], required: true },
    createdAt:  { type: Date, default: Date.now, expires: 60 * 60 * 24 * 30 },
  },
  { timestamps: false }
);

const MqttPayloadSchema = new Schema(
  {
    userId:     { type: Schema.Types.ObjectId, ref: "User",     required: true, index: true },
    mqttUserId: { type: Schema.Types.ObjectId, ref: "MqttUser", required: true, index: true },
    clientName: { type: String, required: true },
    topic:      { type: String, required: true },
    payload:    { type: String, default: "", maxlength: 4000 },
    createdAt:  { type: Date, default: Date.now, expires: 60 * 60 * 24 * 7 },
  },
  { timestamps: false }
);

const MqttUser     = mongoose.model("MqttUser", MqttUserSchema);
const MqttClient   = mongoose.model("MqttClient", MqttClientSchema);
const MqttActivity = mongoose.model("MqttActivity", MqttActivitySchema);
const MqttPayload  = mongoose.model("MqttPayload", MqttPayloadSchema);

module.exports = { MqttUser, MqttClient, MqttActivity, MqttPayload };
