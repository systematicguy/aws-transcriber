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
import {PythonFunction} from '@aws-cdk/aws-lambda-python-alpha';


const TIMEZONE = 'Europe/Zurich';

export class TranscriberStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const prefixedName = "transcriber-dev";

    const shortS3LifecycleRuleSettings = {
      lifecycleRules: [
        {
          expiration: cdk.Duration.days(7),
          enabled: true,
        }
      ]
    }

    const longS3LifecycleRuleSettings = {
      lifecycleRules: [
        {
          expiration: cdk.Duration.days(30),
          enabled: true,
        }
      ]
    }

    const uploadBucket = new s3.Bucket(this, 'UserUploadBucket', {
      bucketName: `${prefixedName}-${this.account}-user-upload`,
      eventBridgeEnabled: true,

      removalPolicy: cdk.RemovalPolicy.DESTROY,
      autoDeleteObjects: true,

      ...shortS3LifecycleRuleSettings,
    });

    const audioBucket = new s3.Bucket(this, 'AudioInputBucket', {
      bucketName: `${prefixedName}-${this.account}-audio-input`,

      removalPolicy: cdk.RemovalPolicy.DESTROY,
      autoDeleteObjects: true,

      ...shortS3LifecycleRuleSettings,
    });

    const transcriptionOutputBucket = new s3.Bucket(this, 'TranscriptionOutputBucket', {
      bucketName: `${prefixedName}-${this.account}-transcription-output`,

      removalPolicy: cdk.RemovalPolicy.DESTROY,
      autoDeleteObjects: true,

      ...shortS3LifecycleRuleSettings,
    });

    const textOutputBucket = new s3.Bucket(this, 'TextOutputBucket', {
      bucketName: `${prefixedName}-${this.account}-text-output`,

      removalPolicy: cdk.RemovalPolicy.RETAIN,
      ...longS3LifecycleRuleSettings,
    });

    const powerToolsLayer = lambda.LayerVersion.fromLayerVersionArn(this, 'PowertoolsLayer',
      `arn:aws:lambda:${this.region}:017000801446:layer:AWSLambdaPowertoolsPythonV2:79`);

    const commonLambdaProps = {
      runtime: lambda.Runtime.PYTHON_3_12,
      layers: [powerToolsLayer],
      timeout: cdk.Duration.minutes(14),  // large files can take a long time to process
    }

    // *****************************************************************************************************************
    // Lambda Function to process uploaded files
    // https://docs.aws.amazon.com/cdk/api/v2/docs/aws-lambda-python-alpha-readme.html
    const processFileLambda = new PythonFunction(this, 'ProcessUploadedAudioFileLambda', {
      functionName: `${prefixedName}-process-uploaded-audio`,

      entry: path.join(__dirname, 'lambda/process_uploaded_audio/'),
      index: 'process_uploaded_audio.py',
      handler: 'handler',

      environment: {
        JOB_INPUT_BUCKET: audioBucket.bucketName,
        TIMEZONE: TIMEZONE,
      },

      ...commonLambdaProps,
    });

    uploadBucket.grantReadWrite(processFileLambda);
    audioBucket.grantReadWrite(processFileLambda);

    // *****************************************************************************************************************
    // Lambda Function to convert transcription to srt
    const processTranscriptLambda = new PythonFunction(this, 'ProcessTranscriptLambda', {
      functionName: `${prefixedName}-process-transcript`,

      entry: path.join(__dirname, 'lambda/process_transcript/'),
      index: 'process_transcript.py',
      handler: 'handler',

      environment: {
        DESTINATION_BUCKET: textOutputBucket.bucketName,
      },

      ...commonLambdaProps,
    });

    transcriptionOutputBucket.grantRead(processTranscriptLambda);
    textOutputBucket.grantReadWrite(processTranscriptLambda);

    // *****************************************************************************************************************
    // step function
    const transcribeRole = new iam.Role(this, 'TranscribeRole', {
      assumedBy: new iam.ServicePrincipal('states.amazonaws.com'),
    });

    audioBucket.grantRead(transcribeRole);
    transcriptionOutputBucket.grantReadWrite(transcribeRole);

    // Step Function Task: Lambda to Process Audio
    const processFileTask = new tasks.LambdaInvoke(this, 'ProcessUploadedFile', {
      lambdaFunction: processFileLambda,
      outputPath: '$.Payload', // Extracts the payload from the Lambda's response
    });

    // https://docs.aws.amazon.com/transcribe/latest/APIReference/API_StartTranscriptionJob.html
    const startTranscriptionTask = new tasks.CallAwsService(this, 'StartTranscription', {
      service: 'transcribe',
      action: 'startTranscriptionJob',
      parameters: {
        // map lambda output to the parameters  // TODO: take all field from the lambda output instead
        TranscriptionJobName: stepfunctions.JsonPath.stringAt('$.TranscriptionJobName'),
        MediaFormat: stepfunctions.JsonPath.stringAt('$.MediaFormat'),
        Media: {
          MediaFileUri: stepfunctions.JsonPath.stringAt('$.Media.MediaFileUri'),
        },
        OutputKey: stepfunctions.JsonPath.stringAt('$.OutputKey'),

        // ----------------------------------------------
        // lambda-independent parameters

        // https://docs.aws.amazon.com/transcribe/latest/APIReference/API_StartTranscriptionJob.html#transcribe-StartTranscriptionJob-request-IdentifyMultipleLanguages
        LanguageCode: 'en-US', // hu-HU is not supported for identify multiple languages

        // https://docs.aws.amazon.com/transcribe/latest/APIReference/API_StartTranscriptionJob.html#transcribe-StartTranscriptionJob-request-OutputBucketName
        OutputBucketName: transcriptionOutputBucket.bucketName,
      },
      iamResources: ['*'], // TODO check how this works
    });

    // Wait Task
    const waitTask = new stepfunctions.Wait(this, 'WaitForTranscription', {
      time: stepfunctions.WaitTime.duration(cdk.Duration.seconds(10)),
    });

    // Task to check the status of the transcription job
    const checkTranscriptionStatusTask = new tasks.CallAwsService(this, 'CheckTranscriptionStatus', {
      service: 'transcribe',
      action: 'getTranscriptionJob',
      parameters: {
        TranscriptionJobName: stepfunctions.JsonPath.stringAt('$.TranscriptionJob.TranscriptionJobName'),
      },
      iamResources: ['*'],
      resultPath: '$.jobStatus', // Store the result in the 'jobStatus' field
    });

    // Step Function Task: Lambda to Process Transcript
    const processTranscriptTask = new tasks.LambdaInvoke(this, 'ProcessTranscript', {
      lambdaFunction: processTranscriptLambda,
    });

    // Choice State to determine job status
    const checkJobStatusChoice = new stepfunctions.Choice(this, 'Is Job Complete?');

    // Define the logic to check job status
    checkJobStatusChoice
      .when(
        stepfunctions.Condition.stringEquals('$.jobStatus.TranscriptionJob.TranscriptionJobStatus', 'COMPLETED'),
        processTranscriptTask
      )
      .when(
        stepfunctions.Condition.stringEquals('$.jobStatus.TranscriptionJob.TranscriptionJobStatus', 'FAILED'),
        new stepfunctions.Fail(this, 'Job Failed', {
          cause: 'Transcription Job Failed',
          error: 'JobStatus: FAILED',
        })
      )
      .otherwise(waitTask.next(checkTranscriptionStatusTask)); // Loop back if the job is still in progress

    const taskChain = processFileTask
      .next(startTranscriptionTask)
      .next(checkTranscriptionStatusTask)
      .next(checkJobStatusChoice);

    const stateMachine = new stepfunctions.StateMachine(this, 'TranscriptionStateMachine', {
      stateMachineName: prefixedName,
      definitionBody: stepfunctions.DefinitionBody.fromChainable(taskChain),
      role: transcribeRole,
    });

    // EventBridge Rule to trigger the Step Function
    new events.Rule(this, 'OnFileUpload', {
      ruleName: `${prefixedName}-onfileupload`,
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
