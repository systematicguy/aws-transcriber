/** MIT License
 * Copy it, but attribute the author: David Horvath - systematicguy.com
 * Linking to the repo is also fine.
 */

import * as cdk from 'aws-cdk-lib';
import {Construct} from 'constructs';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as path from 'path';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as tasks from 'aws-cdk-lib/aws-stepfunctions-tasks';
import * as events from 'aws-cdk-lib/aws-events';
import * as targets from 'aws-cdk-lib/aws-events-targets';
import * as stepfunctions from 'aws-cdk-lib/aws-stepfunctions';



const TIMEZONE = 'Europe/Zurich';

export class TranscriberStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const prefixedName = "transcriber-dev";

    const uploadBucket = new s3.Bucket(this, 'UserUploadBucket', {
      bucketName: `${prefixedName}-${this.account}-user-upload`,
    });

    const audioBucket = new s3.Bucket(this, 'AudioInputBucket', {
      bucketName: `${prefixedName}-${this.account}-audio-input`,
    });

    const outputBucket = new s3.Bucket(this, 'TranscriptionOutputBucket', {
      bucketName: `${prefixedName}-${this.account}-transcription-output`,
    });

    // *****************************************************************************************************************
    // Lambda Function to process uploaded files
    const processFileLambda = new lambda.Function(this, 'ProcessUploadedAudioFileLambda', {
      functionName: `${prefixedName}-process-uploaded-audio`,
      runtime: lambda.Runtime.PYTHON_3_12,
      code: lambda.Code.fromAsset(path.join(__dirname, 'lambda/process_uploaded_audio/')),
      handler: 'process_uploaded_audio.handler',
      environment: {
        JOB_INPUT_BUCKET: audioBucket.bucketName,
        TIMEZONE: TIMEZONE,
      },
    });

    uploadBucket.grantReadWrite(processFileLambda);
    audioBucket.grantReadWrite(processFileLambda);

    // *****************************************************************************************************************
    // step function
    const transcribeRole = new iam.Role(this, 'TranscribeRole', {
      assumedBy: new iam.ServicePrincipal('states.amazonaws.com'),
    });

    transcribeRole.addToPolicy(new iam.PolicyStatement({
      actions: [
        'transcribe:StartTranscriptionJob',
        'transcribe:GetTranscriptionJob'
      ],
      resources: ['*'],
    }));
    outputBucket.grantReadWrite(transcribeRole);

    // Step Function Task: Lambda to Process File
    const processFileTask = new tasks.LambdaInvoke(this, 'ProcessUploadedFile', {
      lambdaFunction: processFileLambda,
      outputPath: '$.Payload', // Extracts the payload from the Lambda's response
    });

    // https://docs.aws.amazon.com/transcribe/latest/APIReference/API_StartTranscriptionJob.html
    const startTranscriptionTask = new tasks.CallAwsService(this, 'StartTranscription', {
      service: 'transcribe',
      action: 'startTranscriptionJob',
      parameters: {
        // TODO: take all field from the lambda output

        // map lambda output to the parameters
        TranscriptionJobName: stepfunctions.JsonPath.stringAt('$.TranscriptionJobName'),
        MediaFormat: stepfunctions.JsonPath.stringAt('$.MediaFormat'),
        Media: {
          MediaFileUri: stepfunctions.JsonPath.stringAt('$.Media.MediaFileUri'),
        },
        OutputKey: stepfunctions.JsonPath.stringAt('$.OutputKey'),

        // ----------------------------------------------
        // lambda-independent parameters
        // https://docs.aws.amazon.com/transcribe/latest/APIReference/API_StartTranscriptionJob.html#transcribe-StartTranscriptionJob-request-IdentifyMultipleLanguages
        IdentifyMultipleLanguages: true,
        LanguageOptions: ['en-US', 'hu-HU'],

        // https://docs.aws.amazon.com/transcribe/latest/APIReference/API_StartTranscriptionJob.html#transcribe-StartTranscriptionJob-request-OutputBucketName
        OutputBucketName: outputBucket.bucketName,
      },
      iamResources: ['*'], // TODO check how this works
    });

    const taskChain = processFileTask
      .next(startTranscriptionTask);

    // Create the State Machine
    const stateMachine = new stepfunctions.StateMachine(this, 'TranscriptionStateMachine', {
      stateMachineName: prefixedName,
      definitionBody: stepfunctions.DefinitionBody.fromChainable(taskChain),
      role: transcribeRole,
    });

    // EventBridge Rule to trigger the Step Function
    new events.Rule(this, 'OnFileUpload', {
      eventPattern: {
        source: ['aws.s3'],
        detailType: ['Object Created'],
        detail: {
          bucket: {
            name: [uploadBucket.bucketName],
          },
        },
      },
      targets: [new targets.SfnStateMachine(stateMachine)],
    });
  }
}
