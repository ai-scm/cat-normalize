import { FoundationStackConfig } from '../stacks/foundation-stack';

/**
 * Test Environment Configuration
 */
export const testFoundationConfig: FoundationStackConfig = {
  environment: 'test',
  bucketName: 'cat-test-normalize-reports',
  enableVersioning: true,
  tags: {
    AutoShutdown: 'true',
  },
};

/**
 * Production Environment Configuration
 */
export const prodFoundationConfig: FoundationStackConfig = {
  environment: 'prod',
  bucketName: 'cat-prod-normalize-reports',
  enableVersioning: true,
  tags: {
    BusinessUnit: 'CATIA-OPERATIONS',
    Criticality: 'High',
  },
};
