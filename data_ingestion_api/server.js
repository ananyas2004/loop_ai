const express = require('express');
const bodyParser = require('body-parser');
const { v4: uuidv4 } = require('uuid');
const Queue = require('bull');

const app = express();
app.use(bodyParser.json());

// Priority values
const PRIORITY = {
  HIGH: 1,
  MEDIUM: 2,
  LOW: 3
};

// In-memory store for ingestion statuses
const ingestionStatuses = new Map();

// Create a queue for processing batches
const processingQueue = new Queue('batch processing', {
  limiter: {
    max: 1,    // Process one batch at a time
    duration: 5000 // Every 5 seconds
  }
});

// POST /ingest endpoint
app.post('/ingest', (req, res) => {
  const { ids, priority } = req.body;
  
  // Validate input
  if (!ids || !Array.isArray(ids) || ids.length === 0 || !priority || !PRIORITY[priority]) {
    return res.status(400).json({ error: 'Invalid request body' });
  }
  
  // Validate IDs
  for (const id of ids) {
    if (typeof id !== 'number' || id < 1 || id > 1e9 + 7) {
      return res.status(400).json({ error: `Invalid ID: ${id}. Must be between 1 and 10^9+7` });
    }
  }
  
  // Create a unique ingestion ID
  const ingestionId = uuidv4();
  
  // Split IDs into batches of 3
  const batches = [];
  for (let i = 0; i < ids.length; i += 3) {
    batches.push({
      batch_id: uuidv4(),
      ids: ids.slice(i, i + 3),
      status: 'yet_to_start'
    });
  }
  
  // Store ingestion status
  ingestionStatuses.set(ingestionId, {
    priority: PRIORITY[priority],
    batches: batches,
    createdAt: new Date(),
    overallStatus: 'yet_to_start'
  });
  
  // Add each batch to the queue with appropriate priority
  batches.forEach((batch, index) => {
    processingQueue.add(
      { ingestionId, batch, batchIndex: index },
      { 
        priority: PRIORITY[priority],
        delay: index * 5000 // Stagger batches by 5 seconds
      }
    );
  });
  
  res.json({ ingestion_id: ingestionId });
});

// Process batches from the queue
processingQueue.process(async (job) => {
  const { ingestionId, batch, batchIndex } = job.data;
  const ingestion = ingestionStatuses.get(ingestionId);
  
  if (!ingestion) {
    throw new Error('Ingestion not found');
  }
  
  // Update batch status to 'triggered'
  ingestion.batches[batchIndex].status = 'triggered';
  updateOverallStatus(ingestion);
  
  // Process each ID in the batch (simulate API calls)
  const results = await Promise.all(batch.ids.map(async (id) => {
    // Simulate API call delay (random between 500-1500ms)
    await new Promise(resolve => setTimeout(resolve, 500 + Math.random() * 1000));
    return { id, data: "processed" };
  }));
  
  // Update batch status to 'completed'
  ingestion.batches[batchIndex].status = 'completed';
  updateOverallStatus(ingestion);
  
  return results;
});

// Helper function to update overall status
function updateOverallStatus(ingestion) {
  const batchStatuses = ingestion.batches.map(b => b.status);
  
  if (batchStatuses.every(status => status === 'completed')) {
    ingestion.overallStatus = 'completed';
  } else if (batchStatuses.some(status => status === 'triggered')) {
    ingestion.overallStatus = 'triggered';
  } else if (batchStatuses.some(status => status === 'yet_to_start')) {
    ingestion.overallStatus = 'yet_to_start';
  }
}

// GET /status/:ingestion_id endpoint
app.get('/status/:ingestion_id', (req, res) => {
  const { ingestion_id } = req.params;
  const ingestion = ingestionStatuses.get(ingestion_id);
  
  if (!ingestion) {
    return res.status(404).json({ error: 'Ingestion not found' });
  }
  
  // Prepare batch responses without internal metadata
  const batchResponses = ingestion.batches.map(batch => ({
    batch_id: batch.batch_id,
    ids: batch.ids,
    status: batch.status
  }));
  
  res.json({
    ingestion_id: ingestion_id,
    status: ingestion.overallStatus,
    batches: batchResponses
  });
});

// Error handling middleware
app.use((err, req, res, next) => {
  console.error(err.stack);
  res.status(500).json({ error: 'Something went wrong!' });
});

// Log queue events
processingQueue.on('completed', (job, result) => {
  console.log(`Batch completed for ingestion ${job.data.ingestionId}`);
});

processingQueue.on('failed', (job, err) => {
  console.error(`Batch failed for ingestion ${job.data.ingestionId}:`, err);
});

// Start the server
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});