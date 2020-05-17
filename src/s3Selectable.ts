import AWS from "aws-sdk";
import {
  ContinuationEvent,
  RecordsEvent,
  StatsEvent,
  EndEvent,
  SelectObjectContentRequest,
  ProgressEvent,
} from "aws-sdk/clients/s3";
import { StreamingEventStream } from "aws-sdk/lib/event-stream/event-stream";
import { GlueTableToS3KeysMapper } from "./mappers/glueTable.mapper";
import mergeStream from "merge-stream";

type PartialBy<TType, TKey extends keyof TType> = Omit<TType, TKey> & Partial<Pick<TType, TKey>>;

export type SelectStream = StreamingEventStream<{
  Records?: RecordsEvent;
  Stats?: StatsEvent;
  Progress?: ProgressEvent;
  Cont?: ContinuationEvent;
  End?: EndEvent;
}>;

export interface IS3SelectOnTable {
  tableName: string;
  databaseName: string;
  glue: AWS.Glue;
  s3: AWS.S3;
}

export class S3SelectOnTable {
  private s3 = this.params.s3;
  private glue = this.params.glue;
  private mapper = new GlueTableToS3KeysMapper({
    s3: this.s3,
    glue: this.glue,
    databaseName: this.params.databaseName,
    tableName: this.params.tableName,
  });

  constructor(private params: IS3SelectOnTable) {}

  public async selectObjectContent(
    params: PartialBy<SelectObjectContentRequest, "Bucket" | "Key">,
  ): Promise<SelectStream> {
    const { Bucket, Keys } = await this.mapper.getS3Keys();
    const selectStreams = await Promise.all(
      Keys.map((Key: string) => this.getSelectStream({ ...params, Bucket, Key })),
    );
    return mergeStream(...selectStreams.filter(stream => stream !== undefined));
  }

  private async getSelectStream(params: SelectObjectContentRequest): Promise<SelectStream> {
    const stream = await this.s3.selectObjectContent(params).promise();
    if (stream.Payload === undefined) throw new Error(`No select stream for ${params.Key}`);
    return <SelectStream>stream.Payload;
  }
}
