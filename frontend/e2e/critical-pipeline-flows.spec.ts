import { expect, test } from '@playwright/test'

test.describe('critical pipeline journeys', () => {
  test('app shell loads with remote/repository navigation', async ({ page }) => {
    await page.goto('/')

    // Keep assertions text-based to survive small UI refactors.
    const body = page.locator('body')
    await expect(body).toContainText(/remote/i)
    await expect(body).toContainText(/repository/i)
  })

  test('canvas workspace mounts and remains interactive', async ({ page }) => {
    await page.goto('/')

    // Canvas host (React Flow uses this class by default).
    const canvas = page.locator('.react-flow')
    await expect(canvas).toBeVisible()

    // Regression smoke: user can open any visible action button.
    const executeBtn = page.getByRole('button', { name: /execute|run|validate/i }).first()
    if (await executeBtn.isVisible()) {
      await executeBtn.click()
      await page.waitForTimeout(250)
    }
  })
})
