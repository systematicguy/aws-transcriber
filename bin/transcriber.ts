#!/usr/bin/env node

/** MIT License
 * Copy it, but attribute the author: David Horvath - systematicguy.com
 * Linking to the repo is also fine.
 */

import 'source-map-support/register';
import * as cdk from 'aws-cdk-lib';
import { TranscriberStack } from '../lib/transcriber-stack';
import { env } from '../config/account';

const app = new cdk.App();
new TranscriberStack(app, 'TranscriberStack', {
  env: env,
});