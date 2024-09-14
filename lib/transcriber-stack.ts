/** MIT License
 * Copy it, but attribute the author: David Horvath - systematicguy.com
 * Linking to the repo is also fine.
 */

import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as s3 from 'aws-cdk-lib/aws-s3';

export class TranscriberStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const prefixedName = "transcriber-dev";

    const inputBucket = new s3.Bucket(this, 'AudioInputBucket', {
      bucketName: `${prefixedName}-${this.account}-input`,
    });
  }
}
