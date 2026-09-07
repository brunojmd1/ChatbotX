"use client"

import { cn } from "@chatbotx.io/ui/lib/utils"
import Image from "next/image"
import { useEffect, useState } from "react"
import { useTenantSettings } from "@/features/tenant"
import { useCurrentTheme } from "@/hooks/use-current-theme"

type BrandIconProps = {
  alt?: string
  className?: string
}

export const BrandIcon = ({
  alt = "Brand Icon",
  className,
}: BrandIconProps) => {
  const currentTheme = useCurrentTheme()
  const [mounted, setMounted] = useState(false)
  const { logoLightUrl, logoDarkUrl, faviconUrl } = useTenantSettings()

  useEffect(() => {
    setMounted(true)
  }, [])

  if (!mounted) {
    return <div className={cn(className, "h-8 w-auto")} />
  }

  const logoUrl = currentTheme === "dark" ? logoLightUrl : logoDarkUrl

  return (
    <>
      {/* Logo - shown when expanded.
          dark:invert: logoLightUrl/logoDarkUrl are the same asset until a
          dedicated light-on-dark export exists (see settings.ts) — without
          this, the wordmark's dark indigo is unreadable on the dark theme's
          sidebar. Matches the collapsed icon below, which already does this. */}
      <Image
        alt={alt}
        className={cn(
          className,
          "brand-expanded h-8 w-auto group-data-[collapsible=icon]:hidden dark:invert",
        )}
        height={40}
        src={logoUrl}
        width={108}
      />
      {/* Icon - shown when collapsed */}
      <Image
        alt={alt}
        className={cn(
          className,
          "brand-collapsed hidden h-8 w-(--sidebar-width-icon) group-data-[collapsible=icon]:block dark:invert",
        )}
        height={52}
        loading="eager"
        src={faviconUrl}
        width={52}
      />
    </>
  )
}
