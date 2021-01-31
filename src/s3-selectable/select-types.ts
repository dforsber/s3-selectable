import { SelectObjectContentCommandInput, SelectObjectContentEventStream } from "@aws-sdk/client-s3";

import { ITableInfo } from "../mappers/glueTableToS3Keys.mapper";

/*
 * interface for select() method
 */
export interface ISelect {
  selectParams: TS3SelectObjectContent;
  onEventHandler?: (event: TEvents) => void;
  onDataHandler?: (event: Uint8Array) => void;
  onEndHandler?: () => void;
}

export type TEvents = SelectObjectContentEventStream;

export const defaultS3SelectParms = {
  ExpressionType: "SQL",
  OutputSerialization: { JSON: {} },
};

export type TS3SelectObjectContent = PartialBy<
  SelectObjectContentCommandInput,
  "Bucket" | "Key" | "ExpressionType" | "OutputSerialization" | "InputSerialization"
>;

export type PartialBy<TType, TKey extends keyof TType> = Omit<TType, TKey> & Partial<Pick<TType, TKey>>;

export type TS3SelectObjectContentVerified = PartialBy<SelectObjectContentCommandInput, "Key"> & {
  Expression: string;
  ExpressionType: string;
  Bucket: string;
};

export type TS3electObjectContentVerified = Omit<ISelect, "selectParams"> & {
  selectParams: TS3SelectObjectContentVerified;
};

export interface IPreparedSelect extends TS3electObjectContentVerified {
  limit: number;
  s3Keys: string[];
}

export interface IExplainSelect {
  tableInfo: ITableInfo;
  preparedSelect: IPreparedSelect;
  partitionFilter: string;
}
