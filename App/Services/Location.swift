import Contacts
import CoreLocation
import Foundation
import MapKit
import OSLog
import Ontology

private let log = Logger.service("location")

private func structuredAddress(from mapItem: MKMapItem) -> [String: Value]? {
    // MKAddress only exposes fullAddress/shortAddress. Use the deprecated placemark.postalAddress
    // via ObjC runtime to get structured components until MKAddress adds them.
    guard let placemark = mapItem.perform(Selector(("placemark")))?.takeUnretainedValue(),
        placemark.responds(to: Selector(("postalAddress"))),
        let postal = placemark.perform(Selector(("postalAddress")))?.takeUnretainedValue()
            as? CNPostalAddress
    else { return nil }
    var components: [String: Value] = [
        "@type": .string("PostalAddress"),
    ]
    if !postal.street.isEmpty {
        components["streetAddress"] = .string(postal.street)
    }
    if !postal.city.isEmpty {
        components["addressLocality"] = .string(postal.city)
    }
    if !postal.state.isEmpty {
        components["addressRegion"] = .string(postal.state)
    }
    if !postal.postalCode.isEmpty {
        components["postalCode"] = .string(postal.postalCode)
    }
    if !postal.country.isEmpty {
        components["addressCountry"] = .string(postal.country)
    }
    if components.count > 1 {
        return components
    }
    return nil
}

final class LocationService: NSObject, Service, CLLocationManagerDelegate, @unchecked Sendable {
    private let locationManager = {
        let manager = CLLocationManager()
        manager.activityType = .other
        manager.desiredAccuracy = kCLLocationAccuracyKilometer
        manager.distanceFilter = kCLDistanceFilterNone
        manager.pausesLocationUpdatesAutomatically = true
        return manager
    }()
    private var latestLocation: CLLocation?
    private var authorizationContinuation: CheckedContinuation<Void, Error>?

    static let shared = LocationService()

    override init() {
        log.debug("Initializing location service")

        super.init()
        locationManager.delegate = self

        // Check authorization status first to avoid any permission prompts
        let status = locationManager.authorizationStatus
        if (status == .authorizedAlways) && CLLocationManager.locationServicesEnabled() {
            log.debug("Starting location updates with existing authorization...")
            locationManager.startUpdatingLocation()
        }
    }

    deinit {
        log.info("Deinitializing location service, stopping updates...")
        locationManager.stopUpdatingLocation()
    }

    var isActivated: Bool {
        get async {
            return locationManager.authorizationStatus == .authorizedAlways
        }
    }

    func activate() async throws {
        try await withCheckedThrowingContinuation {
            (continuation: CheckedContinuation<Void, Error>) in
            self.authorizationContinuation = continuation
            locationManager.delegate = self

            // Check current authorization status first
            let status = locationManager.authorizationStatus
            switch status {
            case .authorizedWhenInUse, .authorizedAlways:
                // Already authorized, resume immediately
                log.debug("Location access authorized")
                continuation.resume()
                self.authorizationContinuation = nil
            case .denied, .restricted:
                // Already denied, throw error immediately
                log.error("Location access denied")
                continuation.resume(
                    throwing: NSError(
                        domain: "LocationServiceError",
                        code: 7,
                        userInfo: [NSLocalizedDescriptionKey: "Location access denied"]
                    )
                )
                self.authorizationContinuation = nil
            case .notDetermined:
                // Need to request authorization
                log.debug("Requesting location access")
                locationManager.requestWhenInUseAuthorization()
            @unknown default:
                // Handle unknown future cases
                log.error("Unknown location authorization status")
                continuation.resume(
                    throwing: NSError(
                        domain: "LocationServiceError",
                        code: 8,
                        userInfo: [NSLocalizedDescriptionKey: "Unknown authorization status"]
                    )
                )
                self.authorizationContinuation = nil
            }
        }
    }

    var tools: [Tool] {
        Tool(
            name: "location_current",
            description: "Get the user's current location",
            inputSchema: .object(
                properties: [:],
                additionalProperties: false
            ),
            annotations: .init(
                title: "Get Current Location",
                readOnlyHint: true,
                openWorldHint: false
            )
        ) { _ in
            return try await withCheckedThrowingContinuation {
                (continuation: CheckedContinuation<GeoCoordinates, Error>) in
                Task {
                    let status = self.locationManager.authorizationStatus

                    guard status == .authorizedAlways else {
                        log.error("Location access not authorized")
                        continuation.resume(
                            throwing: NSError(
                                domain: "LocationServiceError",
                                code: 1,
                                userInfo: [
                                    NSLocalizedDescriptionKey: "Location access not authorized"
                                ]
                            )
                        )
                        return
                    }

                    // If we already have a recent location, use it
                    if let location = self.latestLocation {
                        continuation.resume(
                            returning: GeoCoordinates(location)
                        )
                        return
                    }

                    // Otherwise, request a new location update
                    self.locationManager.desiredAccuracy = kCLLocationAccuracyHundredMeters
                    self.locationManager.startUpdatingLocation()

                    // Modern timeout pattern using task group
                    let location = await withTaskGroup(of: CLLocation?.self) { group in
                        // Start location monitoring task
                        group.addTask {
                            while self.latestLocation == nil {
                                try? await Task.sleep(nanoseconds: 100_000_000)
                                if Task.isCancelled { return nil }
                            }
                            return self.latestLocation
                        }

                        // Start timeout task
                        group.addTask {
                            try? await Task.sleep(nanoseconds: 10_000_000_000)  // 10 seconds
                            return nil
                        }

                        // Return first non-nil result or nil if timeout
                        for await result in group {
                            group.cancelAll()
                            return result
                        }

                        return nil
                    }

                    self.locationManager.stopUpdatingLocation()

                    if let location = location {
                        continuation.resume(
                            returning: GeoCoordinates(location)
                        )
                    } else {
                        continuation.resume(
                            throwing: NSError(
                                domain: "LocationServiceError",
                                code: 2,
                                userInfo: [NSLocalizedDescriptionKey: "Failed to get location"]
                            )
                        )
                    }
                }
            }
        }

        Tool(
            name: "location_geocode",
            description: "Convert an address to geographic coordinates",
            inputSchema: .object(
                properties: [
                    "address": .string(
                        description: "Address to geocode"
                    )
                ],
                required: ["address"],
                additionalProperties: false
            ),
            annotations: .init(
                title: "Geocode Address",
                readOnlyHint: true,
                openWorldHint: true
            )
        ) { arguments in
            guard let address = arguments["address"]?.stringValue else {
                throw NSError(
                    domain: "LocationServiceError",
                    code: 3,
                    userInfo: [NSLocalizedDescriptionKey: "Invalid address"]
                )
            }

            guard let request = MKGeocodingRequest(addressString: address) else {
                throw NSError(
                    domain: "LocationServiceError",
                    code: 4,
                    userInfo: [NSLocalizedDescriptionKey: "Invalid address for geocoding"]
                )
            }
            let mapItems = try await request.mapItems

            guard let mapItem = mapItems.first else {
                throw NSError(
                    domain: "LocationServiceError",
                    code: 4,
                    userInfo: [
                        NSLocalizedDescriptionKey: "No location found for address"
                    ]
                )
            }

            let location = mapItem.location
            var result: [String: Value] = [
                "@context": .string("https://schema.org"),
                "@type": .string("Place"),
                "geo": .object([
                    "@type": .string("GeoCoordinates"),
                    "latitude": .double(location.coordinate.latitude),
                    "longitude": .double(location.coordinate.longitude),
                ]),
            ]

            if let name = mapItem.name {
                result["name"] = .string(name)
            }

            if let address = structuredAddress(from: mapItem) {
                result["address"] = .object(address)
            } else if let fullAddress = mapItem.address?.fullAddress {
                result["address"] = .object([
                    "@type": .string("PostalAddress"),
                    "streetAddress": .string(fullAddress),
                ])
            }

            return Value.object(result)
        }

        Tool(
            name: "location_reverse-geocode",
            description: "Convert geographic coordinates to an address",
            inputSchema: .object(
                properties: [
                    "latitude": .number(),
                    "longitude": .number(),
                ],
                required: ["latitude", "longitude"]
            ),
            annotations: .init(
                title: "Reverse Geocode Location",
                readOnlyHint: true,
                openWorldHint: true
            )
        ) { arguments in
            guard let latitude = arguments["latitude"]?.doubleValue,
                let longitude = arguments["longitude"]?.doubleValue
            else {
                log.error("Invalid coordinates")
                throw NSError(
                    domain: "LocationServiceError",
                    code: 5,
                    userInfo: [NSLocalizedDescriptionKey: "Invalid coordinates"]
                )
            }

            let location = CLLocation(latitude: latitude, longitude: longitude)
            guard let request = MKReverseGeocodingRequest(location: location) else {
                throw NSError(
                    domain: "LocationServiceError",
                    code: 6,
                    userInfo: [NSLocalizedDescriptionKey: "Invalid coordinates for reverse geocoding"]
                )
            }
            let mapItems = try await request.mapItems

            guard let mapItem = mapItems.first else {
                throw NSError(
                    domain: "LocationServiceError",
                    code: 6,
                    userInfo: [
                        NSLocalizedDescriptionKey: "No address found for location"
                    ]
                )
            }

            var result: [String: Value] = [
                "@context": .string("https://schema.org"),
                "@type": .string("Place"),
                "geo": .object([
                    "@type": .string("GeoCoordinates"),
                    "latitude": .double(latitude),
                    "longitude": .double(longitude),
                ]),
            ]

            if let name = mapItem.name {
                result["name"] = .string(name)
            }

            if let address = structuredAddress(from: mapItem) {
                result["address"] = .object(address)
            } else if let fullAddress = mapItem.address?.fullAddress {
                result["address"] = .object([
                    "@type": .string("PostalAddress"),
                    "streetAddress": .string(fullAddress),
                ])
            }

            return Value.object(result)
        }
    }

    // MARK: - CLLocationManagerDelegate

    func locationManager(_ manager: CLLocationManager, didUpdateLocations locations: [CLLocation]) {
        log.debug("Location manager did update locations")
        if let location = locations.last {
            self.latestLocation = location
        }
    }

    func locationManager(_ manager: CLLocationManager, didFailWithError error: Error) {
        log.error("Location manager failed with error: \(error.localizedDescription)")
    }

    func locationManager(
        _ manager: CLLocationManager,
        didChangeAuthorization status: CLAuthorizationStatus
    ) {
        switch status {
        case .authorizedWhenInUse, .authorizedAlways:
            log.debug("Location access authorized")
            authorizationContinuation?.resume()
            authorizationContinuation = nil
        case .denied, .restricted:
            log.error("Location access denied")
            authorizationContinuation?.resume(
                throwing: NSError(
                    domain: "LocationServiceError",
                    code: 7,
                    userInfo: [NSLocalizedDescriptionKey: "Location access denied"]
                )
            )
            authorizationContinuation = nil
        case .notDetermined:
            log.debug("Location access not determined")
            // Wait for the user to make a choice
            break
        @unknown default:
            log.error("Unknown location authorization status")
            break
        }
    }
}
