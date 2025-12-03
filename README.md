# Garbo Pipeline API

This repository contains an API to interact with the Garbo pipeline for processing company reports. The current scope of the API is to provide endpoints for the validation to query jobs and processes and allow interaction between the Garbo pipeline and the user through validation requests.

The latest run of each Company is shown in the company list.

## Terminology
- **Queue**: A BullMQ queue that holds jobs to be processed. The pipeline consists of many independent queues.
- **Job**: An item in a BullMQ queue. A job contains data (payload) and metadata for processing.
- **Worker**: A BullMQ processor attached to a queue. Each queue has a defined worker that processes jobs from the queue.
- **Pipeline**: A acyclic graph of queues defined by us. The pipeline is defined in the `src/config/pipeline.ts` file.
- **Process**: A whole run of a report through the pipeline. A process is definded by us through a the property `id` in the job data. A process consists of	many jobs.

## API Definition

After running the API, the API definition can be found at [http://localhost:3001/api](http://localhost:3001/api).

## Getting Started

### Deployment in the Garbo Environment 

![Deployment Diagram](./docs/deployment.png)

### Prerequisites

- Node.js (23+)
- npm or yarn
- Running [Garbo Pipeline](https://github.com/Klimatbyran/garbo) (only `npm run dev-workers` necessary)

### Installation

1. Clone the repository
```bash
git clone <repository-url>
cd <repository-directory>
```

2. Install dependencies
```bash
npm install
# or
yarn install
```

3. Start the development server
```bash
npm run dev
# or
yarn dev
```

4. Open your browser and navigate to `http://localhost:3001/api`

## Development

### Project Structure

- `/src`: Source code
  - `/config`: Configuration files
  - `/lib`: General functions and utilities
  - `/routes`: API endpoint definitions
  - `/schemas`: Zod schemas for validation
  - `/services`: Business logic and data processing
- `app.ts`: Express application setup
- `index.ts`: Entry point
