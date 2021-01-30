import { Partition, StorageDescriptor } from "@aws-sdk/client-glue";

// AWS APIs return optional values, thus filter them out to make TS/linter happy
export function notUndefined<T>(x: T | undefined): x is T {
  return x !== undefined;
}

// More complex type guard, for ensuring that the Glue Partition has Values
export type TVerifiedStorageDescriptor = Omit<StorageDescriptor, "Location"> & {
  Location: string;
};

export type TVerifiedPartition = Omit<Partition, "Values" | "StorasgeDescriptor"> & {
  Values: string[];
  StorageDescriptor: TVerifiedStorageDescriptor;
};

export function verifiedPartition(x: Partition): x is TVerifiedPartition {
  return (
    x.Values !== undefined &&
    x.Values.length > 0 &&
    x.StorageDescriptor !== undefined &&
    x.StorageDescriptor.Location !== undefined
  );
}
