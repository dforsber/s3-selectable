"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __generator = (this && this.__generator) || function (thisArg, body) {
    var _ = { label: 0, sent: function() { if (t[0] & 1) throw t[1]; return t[1]; }, trys: [], ops: [] }, f, y, t, g;
    return g = { next: verb(0), "throw": verb(1), "return": verb(2) }, typeof Symbol === "function" && (g[Symbol.iterator] = function() { return this; }), g;
    function verb(n) { return function (v) { return step([n, v]); }; }
    function step(op) {
        if (f) throw new TypeError("Generator is already executing.");
        while (_) try {
            if (f = 1, y && (t = op[0] & 2 ? y["return"] : op[0] ? y["throw"] || ((t = y["return"]) && t.call(y), 0) : y.next) && !(t = t.call(y, op[1])).done) return t;
            if (y = 0, t) op = [op[0] & 2, t.value];
            switch (op[0]) {
                case 0: case 1: t = op; break;
                case 4: _.label++; return { value: op[1], done: false };
                case 5: _.label++; y = op[1]; op = [0]; continue;
                case 7: op = _.ops.pop(); _.trys.pop(); continue;
                default:
                    if (!(t = _.trys, t = t.length > 0 && t[t.length - 1]) && (op[0] === 6 || op[0] === 2)) { _ = 0; continue; }
                    if (op[0] === 3 && (!t || (op[1] > t[0] && op[1] < t[3]))) { _.label = op[1]; break; }
                    if (op[0] === 6 && _.label < t[1]) { _.label = t[1]; t = op; break; }
                    if (t && _.label < t[2]) { _.label = t[2]; _.ops.push(op); break; }
                    if (t[2]) _.ops.pop();
                    _.trys.pop(); continue;
            }
            op = body.call(thisArg, _);
        } catch (e) { op = [6, e]; y = 0; } finally { f = t = 0; }
        if (op[0] & 5) throw op[1]; return { value: op[0] ? op[1] : void 0, done: true };
    }
};
exports.__esModule = true;
var aws_sdk_1 = require("aws-sdk");
var s3_selectable_1 = require("@dforsber/s3-selectable");
function writeDataOut(chunk) {
    var _a, _b;
    if ((_a = chunk.Records) === null || _a === void 0 ? void 0 : _a.Payload)
        process.stdout.write(Buffer.from((_b = chunk.Records) === null || _b === void 0 ? void 0 : _b.Payload).toString());
}
function classBasedExample() {
    var _a, _b, _c;
    return __awaiter(this, void 0, void 0, function () {
        var region, tableParams, glueTable, selectStream;
        return __generator(this, function (_d) {
            switch (_d.label) {
                case 0:
                    console.log("Running with class interface");
                    region = { region: (_a = process.env.AWS_REGION) !== null && _a !== void 0 ? _a : "eu-west-1" };
                    tableParams = {
                        s3: new aws_sdk_1.S3(region),
                        glue: new aws_sdk_1.Glue(region),
                        tableName: (_b = process.env.TABLE_NAME) !== null && _b !== void 0 ? _b : "partitioned_elb_logs",
                        databaseName: (_c = process.env.DATABASE_NAME) !== null && _c !== void 0 ? _c : "default"
                    };
                    glueTable = new s3_selectable_1.S3Selectable(tableParams);
                    return [4 /*yield*/, glueTable.selectObjectContent({
                            ExpressionType: "SQL",
                            InputSerialization: { CSV: {}, CompressionType: "GZIP" },
                            OutputSerialization: { JSON: {} },
                            Expression: "SELECT * FROM S3Object LIMIT 1"
                        })];
                case 1:
                    selectStream = _d.sent();
                    selectStream.on("data", writeDataOut);
                    return [2 /*return*/];
            }
        });
    });
}
// async function nonClassBasedExample(): Promise<void> {
//   console.log("Running with non-class interface");
//   const data = await s3selectable("SELECT * FROM default.partitioned_elb_logs LIMIT 1");
//   console.log(data);
// }
classBasedExample()["catch"](function (err) { return console.log(err); });
// nonClassBasedExample().catch(err => console.log(err));
