import { SelectObjectContentCommandInput, SelectObjectContentEventStream } from "@aws-sdk/client-s3";

export type TEvents = SelectObjectContentEventStream;

export type PartialBy<TType, TKey extends keyof TType> = Omit<TType, TKey> & Partial<Pick<TType, TKey>>;

export type TS3SelectObjectContentVerified = PartialBy<SelectObjectContentCommandInput, "Key"> & {
  Expression: string;
  ExpressionType: string;
  Bucket: string;
};

export type TS3electObjectContentVerified = Omit<ISelect, "selectParams"> & {
  selectParams: TS3SelectObjectContentVerified;
};

export type TS3SelectObjectContent = PartialBy<
  SelectObjectContentCommandInput,
  "Bucket" | "Key" | "ExpressionType" | "OutputSerialization" | "InputSerialization"
>;

export const defaultS3SelectParms = {
  ExpressionType: "SQL",
  OutputSerialization: { JSON: {} },
};

export interface IHandlers {
  onEventHandler?: (event: TEvents) => void;
  onDataHandler?: (event: Uint8Array) => void;
  onEndHandler?: () => void;
}

export interface ISelect extends IHandlers {
  selectParams: TS3SelectObjectContent;
}

export interface IPreparedSelect extends IHandlers {
  selectParams: TS3SelectObjectContentVerified;
  limit: number;
  s3Keys: string[];
}
