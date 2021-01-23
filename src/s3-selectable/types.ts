import {
  InputSerialization,
  OutputSerialization,
  S3,
  SelectObjectContentCommandInput,
  SelectObjectContentEventStream,
} from "@aws-sdk/client-s3";

import { Glue } from "@aws-sdk/client-glue";
import { LogLevel } from "bunyan";

export type PartialBy<TType, TKey extends keyof TType> = Omit<TType, TKey> & Partial<Pick<TType, TKey>>;

export type TSelectaParams = PartialBy<
  SelectObjectContentCommandInput,
  "Bucket" | "Key" | "ExpressionType" | "OutputSerialization" | "InputSerialization"
>;

export type TSelectParamsVerified = PartialBy<SelectObjectContentCommandInput, "Bucket" | "Key"> & {
  Expression: string;
  ExpressionType: string;
};

export type TS3electObjectContentVerified = Omit<ISelectObjectContent, "selectParams"> & {
  selectParams: TSelectParamsVerified;
};

export type TEvents = SelectObjectContentEventStream;

export const defaultS3SelectParms = {
  ExpressionType: "SQL",
  OutputSerialization: { JSON: {} },
};

export interface IS3Selectable {
  tableName: string;
  databaseName: string;
  glue: Glue;
  s3: S3;
  logLevel?: LogLevel;
}

export interface ISelectObjectContent {
  selectParams: TSelectaParams;
  onEventHandler?: (event: TEvents) => void;
  onDataHandler?: (event: Uint8Array) => void;
  onEndHandler?: () => void;
}

export interface IS3selectableNonClass {
  sql: string;
  s3: S3;
  glue: Glue;
  InputSerialization?: InputSerialization;
  OutputSerialization?: OutputSerialization;
  loglevel?: LogLevel;
}
