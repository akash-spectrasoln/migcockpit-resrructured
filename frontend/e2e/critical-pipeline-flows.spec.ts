import { expect, test } from '@playwright/test'

test.describe('critical pipeline journeys', () => {
  test('app shell loads login page', async ({ page }) => {
    await page.goto('/login')

    const body = page.locator('body')
    await expect(body).toContainText(/Welcome Back/i)
    await expect(body).toContainText(/Sign in to continue/i)
    await expect(body).toContainText(/Email Address/i)
    await expect(body).toContainText(/Password/i)
  })

  test('login page shows working form controls', async ({ page }) => {
    await page.goto('/login')

    await expect(page.getByRole('button', { name: /Sign In/i })).toBeVisible()
    await expect(page.getByRole('textbox', { name: /Email Address/i })).toBeVisible()
    await expect(page.locator('input[type="password"]')).toBeVisible()
  })
})
