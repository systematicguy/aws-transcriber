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
      eventBridgeEnabled: true,

      removalPolicy: cdk.RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
    });

    const audioBucket = new s3.Bucket(this, 'AudioInputBucket', {
      bucketName: `${prefixedName}-${this.account}-audio-input`,

      removalPolicy: cdk.RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
    });

    const outputBucket = new s3.Bucket(this, 'TranscriptionOutputBucket', {
      bucketName: `${prefixedName}-${this.account}-transcription-output`,
    });

    const powerToolsLayer = lambda.LayerVersion.fromLayerVersionArn(this, 'PowertoolsLayer',
      `arn:aws:lambda:${this.region}:017000801446:layer:AWSLambdaPowertoolsPythonV2:79`);

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
      layers: [powerToolsLayer]
    });

    uploadBucket.grantReadWrite(processFileLambda);
    audioBucket.grantReadWrite(processFileLambda);

    // *****************************************************************************************************************
    // step function
    const transcribeRole = new iam.Role(this, 'TranscribeRole', {
      assumedBy: new iam.ServicePrincipal('states.amazonaws.com'),
    });

    audioBucket.grantRead(transcribeRole);
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
        OutputBucketName: outputBucket.bucketName,
      },
      iamResources: ['*'], // TODO check how this works
    });

    // Wait Task - waits for 10 seconds
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

    // Choice State to determine job status
    const checkJobStatusChoice = new stepfunctions.Choice(this, 'Is Job Complete?');

    // Define Success and Failure states

    // Define the logic to check job status
    checkJobStatusChoice
      .when(
        stepfunctions.Condition.stringEquals('$.jobStatus.TranscriptionJob.TranscriptionJobStatus', 'COMPLETED'),
        new stepfunctions.Succeed(this, 'Job Success')
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
